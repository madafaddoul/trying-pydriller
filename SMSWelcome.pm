package SMSWelcome;

use strict;
use warnings;
use Dancer2;
use Data::Dumper;
use Data::Dx;
use Time::HiRes;
use Time::Local;
use File::Copy;
use Digest::MD5 qw(md5_hex);
use FindBin;
# use JSON;
use POSIX;
use Array::Compare;
use Invigo qw(app_name=SMSWelcome :DB :LOGGING no_auto_log get_config_value);
use Invigo::Elastic;
use Invigo::Cache;
use Invigo::SMSW::Backend;

# BEGIN {
# 	eval 'use JSON::MaybeXS;'  unless exists &{'encode_json'};
# 	Invigo::log_error( $@ ) if $@;
# }

# $LastChangedRevision: 130868 $ on $LastChangedDate: 2025-07-01 15:51:06 +0300 (Tue, 01 Jul 2025) $ # autofilled by SVN if we enable SVN Propperties -> svn:keywords Revision and Date on the file

use lib ('../../' , '../../inc/' , '../bin/' , '../lib/' );

delete $INC{'roamer.pl'}; require 'roamer.pl';
delete $INC{'stats_functions.pl'}; require 'stats_functions.pl';
delete $INC{'specific.pl'}; require 'specific.pl';
delete $INC{'messages.inc.pl'}; require 'messages.inc.pl';
delete $INC{'custom_groups.inc.pl'}; require 'custom_groups.inc.pl';

set serializer => 'JSON';

our ($dbh, $cache, $config);
use Invigo::API::Error qw(api_error);
use Invigo::API::User qw(user_has_access get_user_info load_access_rights);
use Invigo::API::DbHelpers;
use Invigo::API::Utilities qw($config $cache);

my ($tables_with_custom_operations, $is_ucs2,$countries_changed,$operators_changed);
my ($wabbit, $operation_mode, $add_to_mapfilter);
my ($category_to_groups,$event_type_to_state_hash, $state_to_event_hash, $message_categories_to_table_hash, $limit_categories_to_table_hash,$special_message_hash_table, $roaming_types, @roamer_types_requested_order);

hook before => sub {
	Invigo::API::DbHelpers::init_if_needed();
	Invigo::log_debug("Request:".(request->request_uri), {level => 2});
	$config->{elastic} ||= Invigo::Elastic::init();
	($countries_changed,$operators_changed)=reload_if_needed();
};

sub get_wabbit {
	return $wabbit;
}


sub has_access { return user_has_access(session, request, @_) }

sub Init {
	$tables_with_custom_operations = {
		'RT_COUNTRIES' => {'table' => 'TBL_COUNTRIES_NAMES', 'main_column' => 'MCC', 'related_column' => 'MCC'},
		'RT_OPERATORS' => {'table' => 'TBL_OPERATORS_NAMES', 'main_column' => 'HNI', 'related_column' => 'MCC'},
		'TBL_GROUPS_MESSAGES' => {
			'country' => {'table' => 'TBL_GROUP_MEMBERS_COUNTRIES', 'main_column' => 'ID', 'related_column' => 'GROUP_ID'},
			'operator' => {'table' => 'TBL_GROUP_MEMBERS_OPERATORS', 'main_column' => 'ID', 'related_column' => 'GROUP_ID'},
			'region' => {'table' => 'TBL_GROUP_MEMBERS_REGIONS', 'main_column' => 'ID', 'related_column' => 'GROUP_ID'}
		},
		'TBL_BULK_MESSAGES' => {'table' => 'TBL_BULK_MESSAGES_TEXTS', 'main_column' => 'ID', 'related_column' => 'MESSAGE_ID'},
	};

	$config->{UPLOAD_FOLDER} = get_config_value('UPLOAD_FOLDER', '/tmp/uploads');
	$config->{APP_FOLDER} = get_config_value('APP_FOLDER', "./");
	$config->{BULK_FOLDER} = get_config_value('BULK_FOLDER', $config->{APP_FOLDER}."/../bulk");
	mkdir $config->{BULK_FOLDER} unless -d $config->{BULK_FOLDER};
	$config->{ImsiModulo} = Invigo::get_config_value("ImsiModulo",1);

	if (!$operation_mode) {
		if (Invigo::get_config_value('DISABLE_RABBITMQ', 'Y') ne 'Y') {
			use Invigo::SMSW::WabbitRPC;
			$wabbit = Invigo::SMSW::WabbitRPC->new();
			$wabbit->init($dbh,$config);
			$operation_mode = 'rmq';
			$add_to_mapfilter = \&send_on_wabbit;
		} else {
			$operation_mode = 'db';
			$add_to_mapfilter = \&add_to_mapfilter;
		}
	}
	my $cache_driver=Invigo::get_config_value('CACHE_DRIVER','');
	$config->{InvigoCache} ||= Invigo::Cache->new({ driver  => $cache_driver });
	$config->{db_backend_enabled}= Invigo::get_config_value('DB_BACKEND_ENABLED',0);
	$config->{smsw_backend} = Invigo::SMSW::Backend->new($dbh, $config, $cache);

	# Initialize elastic indices in case not already in config
	$config->{smsw_sms_events} ||= Invigo::elastic_index('smsw_sms_events');
	$config->{smsw_current} ||= Invigo::elastic_index('smsw_current');
	$config->{smsw_trips} ||= Invigo::elastic_index('smsw_trips');

	$config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} = get_config_value('COUNTRIES_FOREIGN_LANGUAGE_ABBR');	# Write the language_id from RT_LANGUAGES like: 'ar','en'
	$config->{COUNTRIES_FOREIGN_LANGUAGE} = get_value_from_db($dbh, "SELECT NAME FROM RT_LANGUAGE WHERE ID=?", $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR}) if $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR};
	$config->{DEFAULT_MOBILE_NETWORK_PANE} = get_config_value('DEFAULT_MOBILE_NETWORK_PANE','countries'); 	# Possible Values: countries, operators or roaming_hub
	$config->{COUNTRIES_TABLE_INFO} = get_countries_columns();	


	# Roamer Lookup property config
	$config->{LOOKUP_INFO_HEADER_PANE} = get_config_value("LOOKUP_INFO_HEADER_PANE",["msisdn","type","last_seen","status"]);
	$config->{ROAMER_PROFILE} = get_config_value("ROAMER_PROFILE",["msisdn","imsi","home_country_name","home_operator_name","language","subscription","market_code","secondary_imsi"]);
	$config->{ROAMER_CURRENT_LOCATION} = get_config_value("ROAMER_CURRENT_LOCATION",["vis_vlr_location","vis_country_name","vis_operator_name"]);


	$config->{message_ids_excluded_from_blacklist} = Invigo::get_config_hash_csv('MESSAGE_IDS_EXCLUDED_FROM_BLACKLIST','');
	$config->{lazy_load_profiles}=get_config_value('LAZY_LOAD_PROFILES',0);
	$config->{in_roamer_language}=get_config_value('IN_ROAMER_LANGUAGE');
	$config->{out_roamer_language}=get_config_value('OUT_ROAMER_LANGUAGE');
	$config->{bilateral_enabled} = Invigo::get_config_value('BILATERAL_ENABLED', 0);
	$config->{send_all_after_subscription} = Invigo::get_config_value('SEND_ALL_AFTER_SUBSCRIPTION', 0);
	$config->{send_all_after_subscription_delay} =  Invigo::get_config_value('SEND_ALL_AFTER_SUBSCRIPTION_DELAY');
	$config->{promotion_enabled} =  Invigo::get_config_value('PROMOTION_ENABLED', 0);
	$config->{send_generic_message} =  Invigo::get_config_value('SEND_GENERIC_MESSAGE', 0);
	$config->{local_gmt_offset} =  Invigo::get_config_value('LOCAL_GMT_OFFSET', 0);
	$config->{no_limit_on_tariff_msg} =  Invigo::get_config_value('NO_LIMIT_ON_TARIFF_MSG', 0);
	$config->{limit_on_tariff_msg_neighbours} =  Invigo::get_config_value('LIMIT_ON_TARIFF_MSG_NEIGHBOURS', 0);
	$config->{max_sms_per_period_tariff_neighbour} =  Invigo::get_config_value('max_sms_per_period_tariff_neighbour', 0);
	$config->{max_sms_per_period_in} =  Invigo::get_config_value('MAX_SMS_PER_PERIOD_IN', 0);
	$config->{max_sms_per_period_out} =  Invigo::get_config_value('MAX_SMS_PER_PERIOD_OUT', 0);
	$config->{max_sms_per_period_winback} =  Invigo::get_config_value('MAX_SMS_PER_PERIOD_WINBACK', 0);
	$config->{max_sms_per_period_local} =  Invigo::get_config_value('MAX_SMS_PER_PERIOD_LOCAL', 0);
	$config->{max_sms_per_period_thankyou} =  Invigo::get_config_value('MAX_SMS_PER_PERIOD_THANKYOU', 0);
	$config->{max_sms_per_period_dummy} =  Invigo::get_config_value('MAX_SMS_PER_PERIOD_DUMMY', 0);
	$config->{max_sms_per_period_welcomeback} =  Invigo::get_config_value('MAX_SMS_PER_PERIOD_WELCOMEBACK', 0);
	$config->{ default_expiry_days } = Invigo::get_config_value('DEFAULT_EXPIRY_DAYS');
	$config->{ sms_time_separation_minutes } = Invigo::get_config_value('SMS_TIME_SEPARATION_MINUTES');
	$config->{welcome_sms_expiry}=  Invigo::get_config_value('WELCOME_SMS_EXPIRY');
	$config->{bonvoyage_sms_expiry}=  Invigo::get_config_value('BONVOYAGE_SMS_EXPIRY');
	$config->{winback_sms_expiry} =  Invigo::get_config_value('WINBACK_SMS_EXPIRY');
	$config->{localwelcome_sms_expiry} =  Invigo::get_config_value('LOCALWELCOME_SMS_EXPIRY');
	$config->{thankyou_sms_expiry} =  Invigo::get_config_value('THANKYOU_SMS_EXPIRY');
	$config->{dummy_sms_expiry} =  Invigo::get_config_value('DUMMY_SMS_EXPIRY');
	$config->{welcomeback_sms_expiry} =  Invigo::get_config_value('WELCOMEBACK_SMS_EXPIRY');
	$config->{ require_sms_delivery_report } = Invigo::get_config_value('REQUIRE_SMS_DELIVERY_REPORT',0);
	$config->{log_messages_in_db} = Invigo::get_config_value('LOG_MESSAGES_IN_DB',0);

	$cache->{rt_language} = get_table_hash($dbh,'RT_LANGUAGE',{id => 'ID',cols=>'NAME'});
	$cache->{mcc_hni_operators} = get_table_hash($dbh, 'RT_OPERATORS', {id => ['MCC','HNI'], cols => 'NAME', force_one_col => 1}); # get hni and mcc map for simulations

	init_smsw_messages_app($dbh);
	$config->{manage_cell_id_in} = get_config_value('MANAGE_CELL_ID_IN', 0);
	$config->{manage_cell_id_out} = get_config_value('MANAGE_CELL_ID_OUT', 0);
	
	$cache->{rt_country_codes} = get_table_hash($dbh, 'RT_COUNTRY_CODES', {id => 'MCC', cols => 'CODE'});
	$cache->{rt_inout} = get_table_hash($dbh, 'RT_INOUT', {id => 'ID', cols => 'NAME'});
	$cache->{rt_visitingback} = get_table_hash($dbh, 'RT_VISITINGBACK', {id => 'ID', cols => 'NAME'});
	$cache->{tbl_region} = get_table_hash($dbh, 'TBL_REGION', {id => 'ID', cols => 'NAME'});
	$cache->{rt_subscription} = get_table_hash($dbh, 'RT_SUBSCRIPTION', {id => 'ID', cols => 'NAME'});
	$cache->{tbl_flight_mcc} ||= Invigo::get_rows_from_db($dbh, 'SELECT * FROM TBL_FLIGHT_MCC');

	$config->{in_send_sms} = Invigo::get_config_value('IN_SEND_SMS',0);
	$config->{out_send_sms} = Invigo::get_config_value('OUT_SEND_SMS',0);
	$config->{local_send_sms} = Invigo::get_config_value('LOCAL_SEND_SMS',0);
	$config->{national_send_sms} = Invigo::get_config_value('NATIONAL_SEND_SMS',0);

	$config->{enabled_roaming_types} =  Invigo::get_config_hash_csv('SMSW_ENABLED_ROAMING_TYPES', join(',',keys %{$cache->{rt_inout}}), ',');
	$config->{enabled_roaming_states} =  Invigo::get_config_hash_csv('SMSW_ENABLED_ROAMING_STATES', join(',',keys %{$cache->{rt_visitingback}}), ',');

	$message_categories_to_table_hash = {'VIP'=>{tbl=>'TBL_PROVISIONED_SUBS',key=>'MSISDN'},'Countries'=>{tbl=>'TBL_COUNTRIES_MESSAGES',key=>'MCC'},'Operators'=>{tbl=>'TBL_OPERATORS_MESSAGES',key=>'HNI'},'Default'=>{tbl=>'TBL_DEFAULT_MESSAGE',key=>''}};
	# $message_categories_to_table_hash->{'Group'}={tbl=>'TBL_GROUPS_MESSAGES',key=>''} if $config->{send_group_messages};
	$message_categories_to_table_hash->{'Generic'}={tbl=>'TBL_GENERIC_MESSAGES',key=>''} if $config->{send_generic_message};
	$message_categories_to_table_hash->{'Cell_Groups'}={tbl=>'TBL_CELL_GROUPS_MESSAGES',key=>'GROUP_ID'} if $config->{manage_cell_id_in} || $config->{manage_cell_id_out};

 	$special_message_hash_table = { embassy => { table => 'TBL_EMBASSIES', message_attribute => "specific_message", criteria => ["mcc","hni","lang"]}}; #make sure criteria is ordered correctly like db
 	$special_message_hash_table->{region} = { table => 'TBL_REGION_MESSAGES', message_attribute => "specific_message", criteria => ["visiting_back","region_id","lang"],message_type_required=>1};
	$special_message_hash_table->{tariff} = {table => 'TBL_TARIFF_MESSAGES',message_attribute => "specific_message",criteria => ["mcc","hni","subscription_id","lang"]} if $config->{append_to_extra_vars}->{tariff} && !$config->{country_zones_enabled};
	$special_message_hash_table->{tariff} = {table => 'TBL_TARIFF_MESSAGES',message_attribute => "specific_message",criteria => ["zone_id","subscription_id","lang"]} if $config->{append_to_extra_vars}->{tariff} && $config->{country_zones_enabled};

	$limit_categories_to_table_hash = {'VIP'=>'MSISDN','Countries'=>'MCC','Operators'=>'HNI','Default'=>'','Subscription'=>'subscription'};
	$limit_categories_to_table_hash->{'Cell ID Group'}='cell_id_group' if ($config->{manage_cell_id_in} || $config->{manage_cell_id_out});

	$event_type_to_state_hash = {'welcome'=>'I,V','bon_voyage'=>'O,V','outbound_back'=>'O,B','all'=>'A,A'};
	$event_type_to_state_hash->{'welcomeback'}='I,W' if $config->{send_dummy_sms};
	$event_type_to_state_hash->{'thank_you'}='I,B' if $config->{thank_you_send_sms};
	$event_type_to_state_hash->{'winback'}='I,K' if $config->{send_dummy_sms};
	$event_type_to_state_hash->{'local_welcome'}='L,V' if $config->{local_send_sms};
	$event_type_to_state_hash->{'local_back'}='L,B' if $config->{local_back_send_sms} && $config->{local_send_sms};
	$event_type_to_state_hash->{'national_welcome'}='N,V' if $config->{national_send_sms};
	$event_type_to_state_hash->{'national_back'}='N,B' if $config->{national_send_sms} && $config->{local_back_send_sms};
	%{$state_to_event_hash}=reverse %$event_type_to_state_hash;

	$category_to_groups={countries=>'MCC',operators=>"HNI",imsis=>'IMSI'};
	$category_to_groups->{cell_groups}='CELL_ID' if $config->{manage_cell_id_in} || $config->{manage_cell_id_out};

	$roaming_types = {'inbound' => 'I', 'outbound' => 'O','all'=>'A'};
	$roaming_types->{'local'} = 'L' if $config->{local_send_sms};
	$roaming_types->{'national'} = 'N' if $config->{national_send_sms};

	$roaming_types = { map { $_ => $roaming_types->{$_} } grep {exists $config->{enabled_roaming_types}->{$roaming_types->{$_}}} keys %$roaming_types };
	@roamer_types_requested_order = ('outbound','inbound','local','national','all');

	$config->{sms_list_max} = get_config_value('SMS_LIST_MAX', 5); 
	$config->{countries_group_max_nb} = Invigo::get_config_value('COUNTRIES_GROUP_MAX_NB',10);
	$config->{operators_group_max_nb} = Invigo::get_config_value('OPERATORS_GROUP_MAX_NB',10);
	
	$cache->{rt_roaming_product_id_to_name} = get_table_hash($dbh, 'RT_ROAMING_PRODUCT', {id => 'ID', cols => 'NAME'});
	####################################################################################################################
	# $config->{send_inbound_refresh_message} = Invigo::get_config_value('SEND_INBOUND_REFRESH_MESSAGE', 0);
	# $config->{send_outbound_refresh_message} = Invigo::get_config_value('SEND_OUTBOUND_REFRESH_MESSAGE', 0);
	####################################################################################################################

	$cache->{operator_prefixes}=cache_tbl_operator_numbers_by_hni($dbh);
	$cache->{all_operator_prefixes}=get_table_hash($dbh, 'TBL_OPERATOR_NUMBERS', {id => 'prefix', cols => 'hni'});

	$config->{TEMP_DOWNLOAD_FOLDER} = get_config_value('TEMP_DOWNLOAD_FOLDER', '/tmp/downloads');
	$config->{stats_folder} = get_config_value('STATS_FOLDER');

	#Do not add any more checks on hni for init use the following function for operator specific initializations
	if (defined &operator_smswelcome_init) {
		operator_smswelcome_init($dbh);
	}else { 
		#for backward compatibility, make sure that these operators have an initialization part in their specific file
		die "Operator should have initialization part inside its specific file. Please update it." if ($HNI eq '25099' || $HNI eq '21890' || $HNI eq '60204' || $HNI eq '41405' || $HNI eq '28204' 
																									|| $HNI eq '28301' || $HNI eq '41503' || $HNI eq '51089' || $HNI eq '34001' || $HNI eq '20210');
	}

	$is_ucs2 = {ar => 1, ru => 1, el => 1, hy => 1};
}

sub clean_msisdn {
	local $_ = shift;
	return '' unless $_;
	s/\s//g; #Removes all kinds of spaces
	s/^0+//; #Remove the leading 00 if any
	return '' if !$_ || /\D/;
	return '' if length($_) > 15;
	return operator_clean_msisdn($_) if exists &operator_clean_msisdn;
	return $_;
}

eval {
	require "inc/specific_routes.pl";
};
Invigo::log_debug("No specific_routes loaded") if $@;

=head2 get /config
This call returns the SMSWelcome config variables for the current platform and logged in user
@returns: Result object 

=cut

get '/config' => sub {
	return api_error('NOT_LOGGED_IN')  unless session('username');
	my $specific = {};
	my $operators_countries = Invigo::get_table_hash($dbh, 'RT_OPERATORS', {id => 'HNI', cols => 'MCC'});
	map { $_ =>push @{$specific->{rt_cache}{rt_countries}},{mcc=>$_, name => $cache->{rt_countries}->{$_}} if !($cache->{rt_countries}->{$_} =~ /.*(DO_NOT_USE|DOT_NO_TUSE)/) } keys %{$cache->{rt_countries}};
	map { $_ =>push @{$specific->{rt_cache}{rt_operators}},{hni => $_, name => $cache->{rt_operators}->{$_}, country => $cache->{rt_countries}->{$operators_countries->{$_}}} } keys %{$cache->{rt_operators}};
	map { $_ =>push @{$specific->{rt_cache}{rt_inout}},{id => $_, name => $cache->{rt_inout}->{$_}} } keys %{$cache->{rt_inout}};

	return { result => { smswelcome => $specific } };
};

=head2 /test/
test call
@returns: 

=cut
get '/test' => sub {
	#~ return api_error('ACTION_NOT_ALLOWED')  unless has_access(request , 'esim');

	return { result => "Hello" };
};

=head2 /wabbit/list_queues
This api runs a command => filters the results and returns them with their respective counts
@returns:hash => key is the queue name. Value is the respective count
=cut
get '/wabbit/list_queues' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless has_access("ACCESS_API", 'V') || has_access("/queues_list");

	my $command = `sh -c "sudo rabbitmqctl list_queues"`;
	my @queues = split("\n", $command);
	my $info={};
	foreach (@queues) {
		if($_ =~ /(.+)\s+(\d+)$/){
			$info->{$1} = $2;
		}
	}

	return { result => $info };
};

=head2 /needs_refresh/:cache
This call returns the 0 or 1 in result if the cache in the Vue app needs refresh. 
It also returns the needed cache in the response if refresh is needed
=cut

get '/needs_refresh/:cache' => sub {
	
	my $param = params->{cache};
	return api_error('ACTION_NOT_ALLOWED') unless has_access("ACCESS_API", 'V');
	return api_error('BAD_PARAMETERS') unless $param =~ /^(operators|countries|all)$/;

	return { result => 0 } if !($countries_changed || $operators_changed);
	
	return { result => 1, countries => $cache->{interface_cache}->{countries}} if $param eq 'countries';
	return { result => 1, operators => $cache->{interface_cache}->{operators}} if $param eq 'operators';
	
	return { 
		result => 1, 
		countries => $cache->{interface_cache}->{countries}, 
		operators => $cache->{interface_cache}->{operators}
	};

};

=head2 get /subscription_groups/data
This API is used to get the data needed for the subscriptions page
@returns: Datas needed for the subscriptions page

=cut
get '/subscription_groups/data' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access("/smswelcome/subscription_groups"));
	return {subscriptions=>$cache->{rt_subscription}};
};

=head2 get /get_roamer_info/
This API is used to get the roamer's info from DB
@param: msisdn The MSISDN of the roamer (optional if imsi is provided)
@param: imsi The IMSI of the roamer (optional if msisdn is provided)
@returns: Status of request (OK if successful) and current info of roamer
@errors: MISSING_ROAMER_IDENTIFICATION, ROAMER_NOT_FOUND

=cut
get '/get_roamer_info' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access("/smswelcome/roamers"));
	return api_error('MISSING_ROAMER_IDENTIFICATION') if (!params->{msisdn} && !params->{imsi});

	my ($id, $where);
	my ($sql, $rows, $row, $result);

	# Select roamer identifier
	$id = params->{msisdn};
	$where = 'MSISDN=?';
	if (!$id) {
		$id = params->{imsi};
		$where = 'IMSI=?';
	}

	# Construct sql
	$sql = "SELECT
				TYPE, MSISDN, IMSI,
				TO_CHAR(DATE_IN,'YYYY-MM-DD HH24:MI') AS DATE_IN, TO_CHAR(DATE_LAST,'YYYY-MM-DD HH24:MI') AS DATE_LAST, TO_CHAR(DATE_OUT,'YYYY-MM-DD HH24:MI') AS DATE_OUT,
				HOME_COUNTRY, C1.NAME as HOME_COUNTRY_NAME, HOME_OPERATOR, O1.NAME as HOME_OPERATOR_NAME,
				VIS_COUNTRY, C2.NAME as VIS_COUNTRY_NAME, VIS_OPERATOR, O2.NAME as VIS_OPERATOR_NAME,
				VLR_LOCATION, NUMBER_SMS_SENT 
			FROM TBL_ROAMERS 
				LEFT JOIN RT_COUNTRIES C1 ON C1.MCC = HOME_COUNTRY 
				LEFT JOIN RT_COUNTRIES C2 ON C2.MCC = VIS_COUNTRY 
				LEFT JOIN RT_OPERATORS O1 ON O1.HNI = HOME_OPERATOR 
				LEFT JOIN RT_OPERATORS O2 ON O2.HNI = VIS_OPERATOR 
			WHERE $where
			ORDER BY DATE_LAST DESC";

	# Get roamer info from db
	$rows = Invigo::get_rows_from_db($dbh, $sql, $id, {arrayref_of_hashrefs => 1});
	if (defined($rows) && defined($rows->[0])) {
		$row = $rows->[0];
		$result = "OK";
	} else {
		return api_error('ROAMER_NOT_FOUND');
	}

	return {result => $result, info => $row};
};

=head2 get /get_roamer_history/
This API is used to get the roamer's history from DB
@param: :msisdn The MSISDN of the roamer
@returns: Status of request (OK if successful) and history of roamer
@errors: MISSING_ROAMER_IDENTIFICATION, ROAMER_NOT_FOUND

=cut
get '/get_roamer_history' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access("/smswelcome/roamers"));
	return api_error('MISSING_ROAMER_IDENTIFICATION') if (!params->{msisdn});

	my ($sql, $rows, $result);

	# Construct sql
	$sql = "SELECT 
				EVENT, IN_OUT, VISITING_BACK, MSISDN, IMSI,
				TO_CHAR(DATE_IN, 'YYYY-MM-DD HH24:MI:SS') AS DATE_IN,
				TO_CHAR(DATE_LAST, 'YYYY-MM-DD HH24:MI:SS') AS DATE_LAST,
				TO_CHAR(DATE_OUT, 'YYYY-MM-DD HH24:MI:SS') AS DATE_OUT,
				HOME_COUNTRY, HOME_OPERATOR, VIS_COUNTRY, VIS_OPERATOR,
				VLR_LOCATION, NUMBER_SMS_SENT 
			FROM TBL_ROAMERS_HISTORY 
			WHERE MSISDN=? 
			ORDER BY DATE_IN DESC";

	# Get roamer history from db
	$rows = Invigo::get_rows_from_db($dbh, $sql, params->{msisdn});
	if (defined($rows)) {
		$result = "OK";
	} else {
		return api_error('ROAMER_NOT_FOUND');
	}

	return {result => $result, history => $rows};
};

=head2 get /get_roamer_stats/
This API is used to get the roamer's stats from DB
@param: :msisdn The MSISDN of the roamer
@returns: Status of request (OK if successful) and stats of roamer
@errors: MISSING_ROAMER_IDENTIFICATION, ROAMER_NOT_FOUND

=cut
get '/get_roamer_stats' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access("/smswelcome/roamers"));
	return api_error('MISSING_ROAMER_IDENTIFICATION') if (!params->{msisdn});

	my ($sql, $rows, $row, $result);

	# Construct sql
	$sql = "SELECT 
				IN_OUT AS TYPE, MSISDN, IMSI, DATE_LAST_IN, DATE_LAST_OUT, DURATION_LAST_TRIP,
				TOTAL_TRIPS_DAYS, TOTAL_NUMBER_OF_TRIPS, AVERAGE_DURATION,
				NUM_WSMS_SENT, NUM_WMMS_SENT, NUM_WINBACK_SENT, NUM_BONVOYAGE_SENT, NUM_LOCALSMS_SENT, NUM_BACK_SENT 
			FROM TBL_STATISTICS 
			WHERE MSISDN=?";
	
	# Get roamer stats from db
	$rows = Invigo::get_rows_from_db($dbh, $sql, params->{msisdn}, {arrayref_of_hashrefs => 1});
	if (defined($rows) && defined($rows->[0])) {
		$row = $rows->[0];
		$result = "OK";
	} else {
		return api_error('ROAMER_NOT_FOUND');
	}

	return {result => $result, stats => $row};
};

=head2 get /blacklist/data
This API is used to retrieve the countries data
@returns: Returns Object containing all configs and data needed to start mobile_networks properly 
=cut

get '/blacklist/data' => sub { 
	
	return api_error('ACTION_NOT_ALLOWED') unless (has_access('ACCESS_API','V') || has_access('/blacklist'));
	my @operators;
	my @countries;
	map { $_ =>push @countries,{mcc=>$_, name=>$cache->{rt_countries}->{$_}} if !($cache->{rt_countries}->{$_} =~ /.*(DO_NOT_USE|DOT_NO_TUSE)/)  } keys %{$cache->{rt_countries}};
	map { $_ =>push @operators,{hni=>$_, name=>$cache->{rt_operators}->{$_} }} keys %{$cache->{rt_operators}};
	my $blacklist_types=retreive_blacklist_info_hash({ui_table_cols=>1});

	return {
		countries=>\@countries,
		operators=>\@operators,
		columns=> $blacklist_types,
		message_types=>$event_type_to_state_hash
	};

};

=head2 get /blacklist/:type/rows
This API is used to retrieve the countries data
@returns: Returns Object containing all configs and data needed to start mobile_networks properly 
=cut

get '/blacklist/:type/rows' => sub { 
	
	return api_error('ACTION_NOT_ALLOWED') unless (has_access('ACCESS_API','V') || has_access('/blacklist'));

	my $countries;
	map { $_ => $countries->{$_}=$cache->{rt_countries}->{$_} if !($cache->{rt_countries}->{$_} =~ /.*(DO_NOT_USE|DOT_NO_TUSE)/)  } keys %{$cache->{rt_countries}};

	my ($sql,$res);
	my $blacklist_types=retreive_blacklist_info_hash();
	my $blacklist_type = $blacklist_types->{params->{type}};
	return api_error("Blacklist type invalid") if !defined($blacklist_type);

	if($blacklist_type->{no_msg_id}){
		$sql = "SELECT CONCAT_WS(',',IN_OUT,VISITING_BACK,". join(",",@{[$blacklist_type->{tbl_key}]})."),CONCAT_WS(',',IN_OUT,VISITING_BACK),". join(",",@{[$blacklist_type->{tbl_key}]}) ." FROM ".$blacklist_type->{tbl};
	}else{
		$sql = "SELECT CONCAT_WS(',',IN_OUT,VISITING_BACK,". join(",",@{[$blacklist_type->{tbl_key}]})."),CONCAT_WS(',',IN_OUT,VISITING_BACK),". join(",",@{[$blacklist_type->{tbl_key}]}) ." FROM ".$blacklist_type->{tbl}." WHERE MESSAGE_ID=0";
	}
	$res=get_rows_from_db($sql);
	return api_error("Blacklist row fetch failed") if !$res;

	foreach my $row (@$res){
		$row->[1]=$state_to_event_hash->{$row->[1]};
		if (params->{type} eq 'operators' || params->{type} eq 'countries'){
			if (params->{type} eq 'operators'){
				push @{$row},$countries->{substr $row->[2],0,3};
				push @{$row},$cache->{rt_operators}->{$row->[2]};
			} else{
				push @{$row},$countries->{$row->[2]};
			}
		}
	}
	
	return {rows => $res};
};

############################################# ----- Start of old section ----- #############################################

=head2 post /blacklist/:type
This API is used to blacklist or unblacklist a value of a certain type: MSISDN, IMSI, Country, or Operator.
Blacklisting is done by setting the message ID related to the provided values to 0.
When unblacklisting, of a message ID is provided, it will be updated to that value in the DB.
@param: :type The type of the field to be blacklisted (roamer/country/operator)
@param: msisdn The MSISDN of the roamer to blacklist (only used for type=roamer, optional if imsi is provided)
@param: imsi The IMSI of the roamer to blacklist (only used for type=roamer, optional if msisdn is provided)
@param: mcc The MCC of the country to blacklist (only used for type=country)
@param: hni The HNI of the operator to blacklist (only used for type=operator)
@param: direction The direction of blacklisting, 'in' to blacklist and 'out' to unblacklist (optional, default 'in')
@param: roaming_type The type of roaming (inbound/outbound/local) (optional)
@param: roaming_state The state of roaming (visiting/back) (optional)
@param: lang The language ID (optional)
@param: message_id The message ID (used only if unblacklisting) (optional)
@param: message_type The type of messages to blacklist (all/embassy/bonvoyage) (optional, default 'all')
@returns: Status of request (OK if successful), along with type, direction, and value of blacklisted field
@errors: INVALID_BLACKLIST_TYPE, INVALID_OR_MISSING_FIELD, OPERATION_FAILED

=cut

any ['post','put'] => '/blacklist/:type' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access('API:blacklist' ,'E'));
	return api_error('INVALID_BLACKLIST_TYPE') if (!params->{type});

	my $params=params;
	return api_error('INVALID_OR_MISSING_FIELD') unless validate_blacklist_params($params);

	log_debug("Blacklisting api called with params: ".Dumper ($params));

	# Get params from request
	my $type = params->{type};
	my $direction = params->{direction};
	$direction = 'in' if (!defined($direction) || ($direction ne 'in' && $direction ne 'out'));

	my (@sql_extra, @sql_params, $id, $blacklisted, $res, $table);

	if (defined params->{filename}){
		return api_error("Wrong File format") unless params->{filename}=~ m/\.txt$/;
		my $file_path=$config->{UPLOAD_FOLDER}."/".params->{file};
		open(my $uploadfile, "<", $file_path) or die "$!";
		local $/;
		my @fields = split(",",<$uploadfile>);
		map { s/\s$//g } @fields;
		
		if(scalar grep(!/^\d+$/,@fields) >0){
			unlink($file_path);
			return api_error("file is not formatted correctly");
		}
		params->{lc params->{bulk_type}}=\@fields;
		unlink($file_path);
		my $params=params
		return $params;
	}
	
	# Check type of blacklisting (roamer, country, or operator)
	if ($type eq 'roamer') {
		return api_error('INVALID_OR_MISSING_FIELD') if (!params->{msisdn} && !params->{imsi});

		# Blacklisting imsi
		my $imsi = params->{imsi};
		if (defined($imsi)) {
			if ($direction eq 'in') {
				$res = upsert($dbh, "TBL_BLACKLISTED_PREFIXES", {IMSI_PREFIX => $imsi}, {});
				return api_error('OPERATION_FAILED') if (!$res);
				log_debug("Inserted or Updated $imsi in TBL_BLACKLISTED_PREFIXES");
			} else {
				$res = execute_sql($dbh, "DELETE FROM TBL_BLACKLISTED_PREFIXES WHERE IMSI_PREFIX LIKE '$imsi%'");
				return api_error('OPERATION_FAILED') if (!$res);
				log_debug("Deleted $imsi in TBL_BLACKLISTED_PREFIXES");
			}

			return {result => "OK", type => $type."/imsi", direction => $direction, value => $imsi};
		}

		# Blacklsiting msisdn
		push(@sql_extra, 'MSISDN=?');
		$id = params->{msisdn};
		$table = "TBL_PROVISIONED_SUBS";
		$type .= "/msisdn";
	} elsif ($type eq 'country') {
		return api_error('INVALID_OR_MISSING_FIELD') if (!params->{mcc});

		# Blacklsiting mcc
		push(@sql_extra, 'MCC=?');
		$id = params->{mcc};
		$table = "TBL_COUNTRIES_MESSAGES";
	} elsif ($type eq 'operator') {
		return api_error('INVALID_OR_MISSING_FIELD') if (!params->{hni});

		# Blacklsiting hni
		push(@sql_extra, 'HNI=?');
		$id = params->{hni};
		$table = "TBL_OPERATORS_MESSAGES";
	}

	# Get the rest of params from request
	my $roaming_type = params->{roaming_type};
	return api_error('INVALID_ROAMING_TYPE_FIELD') unless (!defined($roaming_type) || grep(/^$roaming_type/, ('inbound', 'outbound', 'local')));
	my $roaming_state = params->{roaming_state};
	return api_error('INVALID_ROAMING_STATE_FIELD') unless (!defined($roaming_state) || grep(/^$roaming_state/, ('visiting', 'back')));
	my $lang = params->{lang};
	my $message_id = params->{message_id};

	# Add provided params to sql
	push(@sql_params, $id);
	if (defined($roaming_type)) {
		push(@sql_extra, 'IN_OUT=?');
		push (@sql_params, $roaming_types->{$roaming_type});
	}
	if (defined($roaming_state)) {
		push(@sql_extra, 'VISITING_BACK=?');
		push (@sql_params, ($roaming_state eq 'visiting' ? 'V' : 'B'));
	}
	if (defined($lang)) {
		push(@sql_extra, 'LANG=?');
		push (@sql_params, $lang);
	}
	foreach my $excluded_id (keys %{$config->{message_ids_excluded_from_blacklist}}) {
		push (@sql_extra, 'MESSAGE_ID <> ?');
		push (@sql_params, $excluded_id);
	}
	# Check if already blacklisted
	$blacklisted = get_rows_from_db($dbh, "SELECT MESSAGE_ID FROM $table WHERE ".join(" AND ", @sql_extra), @sql_params);
	if ($direction eq 'in') {
		# Only blacklist if not blacklisted
		if (defined($blacklisted) && scalar(@{$blacklisted}) > 0) {
			$res = execute_sql($dbh, "UPDATE $table SET MESSAGE_ID=0 WHERE ".join(" AND ", @sql_extra), @sql_params);
			return api_error('OPERATION_FAILED') if (!$res);
			log_debug("Blacklisted entry in $table");
		}
		else{
			@sql_extra=grep { $_ =~ /.*\=\?/ } @sql_extra;
			map { s/\=\?//} @sql_extra;
			if (defined &operator_specific_blacklist_action) {
				log_debug("Operator specific blacklist action with $table");
				$res = operator_specific_blacklist_action($dbh,$table,$params);
				return api_error('OPERATION_FAILED') if (!$res);
			}
			if(!$res || $res eq 'Default'){
			pop @sql_params if scalar @sql_params gt scalar @sql_extra;
			push (@sql_params, '0');
			push @sql_extra,'MESSAGE_ID';
				$res = execute_sql($dbh, "Insert into $table (".join(',',@sql_extra).") values(".join(',',('?') x scalar @sql_params).")",@sql_params);
				return api_error('OPERATION_FAILED') if (!$res);
				log_debug("Inserted new blacklist entry in $table");
			}
		}
	} else {
		# Only unblacklist if blacklisted
		if (defined($blacklisted) && scalar(@{$blacklisted}) > 0) {
			if (defined($message_id)) {
				$res = execute_sql($dbh, "UPDATE $table SET MESSAGE_ID=".$message_id." WHERE ".join(" AND ", @sql_extra), @sql_params);
				return api_error('OPERATION_FAILED') if (!$res);
				log_debug("Unblacklisted entry in $table with $message_id");
			} else {
				$res = execute_sql($dbh, "DELETE FROM $table WHERE ".join(" AND ", @sql_extra), @sql_params);
				return api_error('OPERATION_FAILED') if (!$res);
				log_debug("Deleted blacklist entry in $table");
			}
		}
	}
	Invigo::set_db_update_time("BLACKLIST_$table");

	return {result => "OK", type => $type, direction => $direction, value => $id};

};

############################################# ----- End of old section ----- #############################################

any ['post','put'] => '/blacklist/web/update/:type' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access('API:blacklist' ,'E'));

	my $blacklist_types=retreive_blacklist_info_hash();
	my $blacklist_type = $blacklist_types->{params->{type}};
	return api_error("Blacklist type invalid") if !defined($blacklist_type);
	
	my $params=params;
	my ($type,$state)= split ',', $event_type_to_state_hash->{params->{message_type}} if defined params->{message_type} && defined $event_type_to_state_hash->{params->{message_type}} && $event_type_to_state_hash->{params->{message_type}};
	my ($old_type,$old_state)= split ',', $event_type_to_state_hash->{params->{old}->{message_type}} if defined params->{old}->{message_type} && defined $event_type_to_state_hash->{params->{old}->{message_type}} && $event_type_to_state_hash->{params->{old}->{message_type}};
	my $sqls_to_execute=[];
	
	if (($type || '') eq 'A' && !$blacklist_type->{single_row}) {
		return api_error("CANNOT_ADD_ALL_TYPE") if get_value_from_db($dbh, "SELECT count(*) FROM ".$blacklist_type->{tbl}." WHERE $blacklist_type->{tbl_key}=?",$params->{old}->{lc $blacklist_type->{tbl} eq 'tbl_blacklisted_prefixes' ? 'imsi' : lc $blacklist_type->{key}}) > 1;
	}

	if($blacklist_type->{no_msg_id}){
		return api_error('INVALID_OR_MISSING_FIELD') if !defined(params->{imsi});
		params->{imsi} = [params->{imsi}] if ref(params->{imsi}) ne 'ARRAY';
		return api_error('INVALID_OR_MISSING_FIELD') if join('',@{params->{imsi}}) !~ /^[\d]+$/;
		push @$sqls_to_execute, ["Delete from ".$blacklist_type->{tbl}." where ".$blacklist_type->{tbl_key}."=? and VISITING_BACK=? and IN_OUT=? ",$params->{old}->{imsi},$old_state,$old_type];
		push @$sqls_to_execute, ["Insert into ".$blacklist_type->{tbl}." (visiting_back,in_out,imsi_prefix) values(?,?,?)",$state,$type,$_] foreach @{$params->{imsi}};
	}
	else{
		return api_error('INVALID_OR_MISSING_FIELD') if !defined(params->{lc $blacklist_type->{tbl_key}}) || !defined(params->{message_type}) || !defined(params->{old}->{message_type});
		params->{lc $blacklist_type->{tbl_key}} = [params->{lc $blacklist_type->{tbl_key}}] if ref(params->{lc $blacklist_type->{tbl_key}}) ne 'ARRAY';
		return api_error('INVALID_OR_MISSING_FIELD') if join('',@{params->{lc $blacklist_type->{tbl_key}}}) !~ /^[\d]+$/;
		push @$sqls_to_execute, ["Delete from ".$blacklist_type->{tbl}." where ".$blacklist_type->{tbl_key}."=? and VISITING_BACK=? and IN_OUT=? and MESSAGE_ID=0;",($params->{old}->{lc $blacklist_type->{key}},$old_state,$old_type)];
		push @$sqls_to_execute, ["Insert into ".$blacklist_type->{tbl}."(message_id,visiting_back,in_out,".$blacklist_type->{tbl_key}.") values(?,?,?,?)",('0',$state,$type,$_)] foreach @{$params->{lc $blacklist_type->{key}}};
	}
	my $res=Invigo::execute_sqls($dbh,@$sqls_to_execute);
	
	return api_error('OPERATION_FAILED') if (!$res);
	Invigo::set_db_update_time("BLACKLIST_".$blacklist_type->{tbl});
	return {res=>$res,result => "OK"};
};

any ['post','put'] => '/blacklist/web/:type' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access('API:blacklist' ,'E'));
	return api_error('INVALID_BLACKLIST_TYPE') if (!params->{type});

	my $params=params;
	my $blacklist_type_info=retreive_blacklist_info_hash();
	log_debug("Blacklisting api called with params: ".Dumper ($params));

	# Get params from request
	my $type = params->{type};
	my $direction = params->{direction};
	my $session_sender = session->{data}->{sender} || 2;	
	$direction = 'in' if (!defined($direction) || ($direction ne 'in' && $direction ne 'out'));

	my (@sql_extra, @sql_params, $id, $blacklisted, $res, $table);

	if (defined params->{filename}){
		return api_error("Wrong File format") unless (params->{filename}=~ m/\.txt$/ || params->{filename}=~ m/\.csv$/);
		my $file_path=$config->{UPLOAD_FOLDER}."/".params->{file};
		if(-e $file_path){
			open(my $uploadfile, "<", $file_path) or die "$!";
			local $/;
			my @fields;
			if (params->{filename}=~ m/\.csv$/){
				@fields  = split("\n",<$uploadfile>);
			}else{
				@fields  = split(/[, \n]/,<$uploadfile>);
			}
			
			map { s/\s$//g } @fields;
			if(scalar grep(!/^\d+$/,@fields) >0){
				unlink($file_path);
				return api_error("file is not formatted correctly");
			}
			params->{$blacklist_type_info->{lc params->{type}}->{key}}=\@fields;
			unlink($file_path);
		}else{
			return api_error('Upload error, please reupload file');
		}
	}
	
	# Check type of blacklisting (roamer, country, or operator)
	if ($type eq 'imsi') {
		return api_error('INVALID_OR_MISSING_FIELD') if (!params->{imsi});

		# Blacklisting imsi
		my $imsi = params->{imsi};
		$imsi= [params->{imsi}] if ref($imsi) ne 'ARRAY';
		return api_error('INVALID_OR_MISSING_FIELD') if (scalar @$imsi eq 0);
		if (defined($imsi)) {
			my $roaming_type = params->{roaming_type};
			return api_error('INVALID_ROAMING_TYPE_FIELD') unless (!defined($roaming_type) || grep(/^$roaming_type/, ('inbound', 'outbound', 'local')));
			my $roaming_state = params->{roaming_state};
			return api_error('INVALID_ROAMING_STATE_FIELD') unless (!defined($roaming_state) || grep(/^$roaming_state/, ('visiting', 'back')));
			my $message_id = params->{message_id};
			# Add provided params to sql
			($roaming_type,$roaming_state)= split ',', $event_type_to_state_hash->{params->{message_type}} if $session_sender eq 2 && defined params->{message_type} && defined $event_type_to_state_hash->{params->{message_type}};
			return api_error("MISSING_ROAMING_TYPE_AND_STATE_FIELDS") if !defined($roaming_state) && !defined($roaming_type);

			my $blacklisted;
			
			if (($roaming_type eq 'A' || uc ($roaming_types->{$roaming_type} || '') eq 'A') && $direction eq 'in'){
				$blacklisted = get_rows_from_db($dbh, "SELECT * FROM tbl_blacklisted_prefixes WHERE IMSI_PREFIX IN (".join(',',map {'?'} @{$imsi}).")", @$imsi,{arrayref_of_hashrefs => 1});
				return api_error("CANNOT_ADD_ALL") if (defined($blacklisted) && scalar(@{$blacklisted}) > 0 && $session_sender eq 2);
			} elsif ($direction eq 'in'){
				$blacklisted = get_rows_from_db($dbh, "SELECT * FROM tbl_blacklisted_prefixes WHERE ((IN_OUT=? AND VISITING_BACK=?) OR (IN_OUT='A' AND VISITING_BACK='A')) AND IMSI_PREFIX IN (".join(',',map {'?'} @{$imsi}).")", $roaming_type, $roaming_state, @$imsi,{arrayref_of_hashrefs => 1});
				return api_error("ALL_ALREADY_EXISTS") if (defined($blacklisted) && scalar(@{$blacklisted}) > 0 && $session_sender eq 2);
			}

			if ($direction eq 'in') {
				my $sqls_to_execute = [];
				foreach (@$imsi){
					if ($session_sender eq 2){
						push @$sqls_to_execute, ["INSERT INTO tbl_blacklisted_prefixes (IN_OUT,VISITING_BACK,IMSI_PREFIX) VALUES (?,?,?)",$roaming_type,$roaming_state,$_];
					}
					else{
						$res = upsert($dbh, "TBL_BLACKLISTED_PREFIXES", { IMSI_PREFIX => $_, IN_OUT=>$roaming_type, VISITING_BACK=>$roaming_state}, {});
					}
				}
				$res = Invigo::execute_sqls($dbh,@$sqls_to_execute) if (scalar(@$sqls_to_execute) > 0);
				return api_error('OPERATION_FAILED') if (!$res);
				log_debug("Inserted or Updated ".join(",",@{$imsi})." in TBL_BLACKLISTED_PREFIXES");
			} else {
				$res = execute_sql($dbh, "DELETE FROM TBL_BLACKLISTED_PREFIXES WHERE IN_OUT=? AND VISITING_BACK=? AND IMSI_PREFIX LIKE ANY(ARRAY[".join(',',map{"'".$_."%'"} @{$imsi})."])",$roaming_type,$roaming_state);
				return api_error('OPERATION_FAILED') if (!$res);
				log_debug("Deleted ".join(",",@{$imsi})." in TBL_BLACKLISTED_PREFIXES");
			}
			Invigo::set_db_update_time("BLACKLIST_TBL_BLACKLISTED_PREFIXES");
			return {result => "OK", type => $type, direction => $direction, value => $imsi};
		}
	}

	return api_error('INVALID_OR_MISSING_FIELD') if !defined $blacklist_type_info->{$type}->{key} || !defined !params->{$blacklist_type_info->{$type}->{key}} || !params->{$blacklist_type_info->{$type}->{key}};
	$id=params->{$blacklist_type_info->{$type}->{key}};
	$id= [params->{$blacklist_type_info->{$type}->{key}}] if ref($id) ne 'ARRAY';
	return api_error('INVALID_OR_MISSING_FIELD') if (scalar @$id eq 0);
	push @sql_extra, $blacklist_type_info->{$type}->{tbl_key}.' IN ('.join(',',map {'?'} @{$id}).')';
	$table = $blacklist_type_info->{$type}->{tbl};

	# Get the rest of params from request
	my $roaming_type = params->{roaming_type};
	return api_error('INVALID_ROAMING_TYPE_FIELD') unless (!defined($roaming_type) || grep(/^$roaming_type/, ('inbound', 'outbound', 'local')));
	my $roaming_state = params->{roaming_state};
	return api_error('INVALID_ROAMING_STATE_FIELD') unless (!defined($roaming_state) || grep(/^$roaming_state/, ('visiting', 'back')));
	my $message_id = params->{message_id};

	# Add provided params to sql
	push(@sql_params, @$id);
	($roaming_type,$roaming_state)= split ',', $event_type_to_state_hash->{params->{message_type}} if $session_sender eq 2 && defined params->{message_type} && defined $event_type_to_state_hash->{params->{message_type}};
	return api_error("MISSING_ROAMING_TYPE_AND_STATE_FIELDS") if !defined($roaming_state) && !defined($roaming_type);

	
	foreach my $excluded_id (keys %{$config->{message_ids_excluded_from_blacklist}}) {
		push (@sql_extra, 'MESSAGE_ID <> ?');
		push (@sql_params, $excluded_id);
	}
	$blacklisted = get_rows_from_db($dbh, "SELECT MESSAGE_ID,".$blacklist_type_info->{$type}->{tbl_key}." FROM $table WHERE ".join(" AND ", @sql_extra), @sql_params,{arrayref_of_hashrefs => 1}) if $roaming_type eq 'A' || uc ($roaming_types->{$roaming_type} || '') eq 'A';

	if (defined($roaming_type)) {
		push(@sql_extra, 'IN_OUT=?');
		push (@sql_params, $roaming_types->{$roaming_type}) if $session_sender ne 2;
		push (@sql_params, $roaming_type) if $session_sender eq 2;
	}
	if (defined($roaming_state)) {
		push(@sql_extra, 'VISITING_BACK=?');
		push (@sql_params, ($roaming_state eq 'visiting' ? 'V' : 'B')) if $session_sender ne 2;
		push (@sql_params, $roaming_state) if $session_sender eq 2;
	}
	# Check if already blacklisted
	$blacklisted = get_rows_from_db($dbh, "SELECT MESSAGE_ID,".$blacklist_type_info->{$type}->{tbl_key}." FROM $table WHERE ".join(" AND ", @sql_extra)." OR (VISITING_BACK='A' AND IN_OUT='A' AND ".$blacklist_type_info->{$type}->{tbl_key}." IN (".join(',',map {'?'} @{$id})."))", @sql_params,@$id,{arrayref_of_hashrefs => 1}) unless $roaming_type eq 'A' || uc ($roaming_types->{$roaming_type} || '') eq 'A';

	if ($direction eq 'in') { 

		my $comp = shift(@sql_extra);
		$comp =~ s/IN \([\?,]+\)|\=\?//;
		$comp .= "=?";
		unshift(@sql_extra,$comp);
		shift(@sql_params) foreach (@$id);

		# Only blacklist if not blacklisted
		if (defined($blacklisted) && scalar(@{$blacklisted}) > 0 && $session_sender ne 2) {

			my $existing_ids = {};
			my $sqls_to_execute = [];
			%$existing_ids = map { $_->{$blacklist_type_info->{$type}->{tbl_key}} => 1 } @$blacklisted;
			
			foreach (@$id){	
				unshift(@sql_params,$_);
				push @$sqls_to_execute, ["UPDATE $table SET MESSAGE_ID=0 WHERE ".join(" AND ", @sql_extra), @sql_params] if exists($existing_ids->{$_});
				push @$sqls_to_execute, ["INSERT INTO $table (".$blacklist_type_info->{$type}->{tbl_key}." ,IN_OUT,VISITING_BACK,MESSAGE_ID) values(?,?,?,0)", @sql_params] unless exists($existing_ids->{$_});
				shift(@sql_params);
			}

			$res = Invigo::execute_sqls($dbh,@$sqls_to_execute);
			return api_error('OPERATION_FAILED') if (!$res);
			log_debug("Blacklisted entries in $table where ".$blacklist_type_info->{$type}->{tbl_key}." equal to ".join(',',@$id));

		}
		else{
			return api_error('OPERATION_FAILED') if (defined($blacklisted) && scalar(@{$blacklisted}) > 0 && $session_sender eq 2);

			@sql_extra=grep { $_ =~ /.*\=\?/ } @sql_extra;
			map { s/\=\?//} @sql_extra;
			if (defined &operator_specific_blacklist_action && $session_sender ne 2) {
				log_debug("Operator specific blacklist action with $table");
				$res = operator_specific_blacklist_action($dbh,$table,$params);
				return api_error('OPERATION_FAILED') if (!$res);
			}
			if(!$res || $res eq 'Default'){
				pop @sql_params if scalar @sql_params gt scalar @sql_extra;
				push (@sql_params, '0');
				push @sql_extra,'MESSAGE_ID';
				
				my $sqls_to_execute = [];
				foreach (@$id){
					unshift(@sql_params,$_);
					push @$sqls_to_execute,["Insert into $table (".join(',',@sql_extra).") values(".join(',',('?') x scalar @sql_params).")",@sql_params];
					shift(@sql_params);
				}
				$res = Invigo::execute_sqls($dbh,@$sqls_to_execute);
				return api_error('OPERATION_FAILED') if (!$res);
				log_debug("Inserted ".scalar(@$sqls_to_execute)." new blacklist entries in $table");
			}
		}
	} else {
		# Only unblacklist if blacklisted
		if (defined($blacklisted) && scalar(@{$blacklisted}) > 0) {
			if (defined($message_id)) {
				$res = execute_sql($dbh, "UPDATE $table SET MESSAGE_ID=".$message_id." WHERE ".join(" AND ", @sql_extra), @sql_params);
				return api_error('OPERATION_FAILED') if (!$res);
				log_debug("Unblacklisted entry in $table with $message_id");
			} else {
				$res = execute_sql($dbh, "DELETE FROM $table WHERE ".join(" AND ", @sql_extra), @sql_params);
				return api_error('OPERATION_FAILED') if (!$res);
				log_debug("Deleted blacklist entry in $table");
			}
		}
	}
	Invigo::set_db_update_time("BLACKLIST_$table");
	return {result => "OK", type => $type, direction => $direction, value => $id};

};

=head2 get /cell_ids/groups
This API is used to list the Cell ID groups in the DB.
@param:
@returns: Status of request (OK if successful) and list of cell groups

=cut
get '/cell_ids/groups' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V'));

	my ($rows, $result);

	# Get defined cell groups from db
	$rows = get_rows_from_db($dbh, "SELECT ID,NAME FROM RT_CELL_GROUPS ORDER BY ID", {arrayref_of_hashrefs => 1});
	if (defined($rows)) {
		$result = "OK";
	}

	return {result => $result, groups => $rows};
};

=head2 post /cell_ids/groups/new
This API is used to add a new Cell ID group in the DB.
@param: id The ID of the new group (ID will be incremeneted if this not provided)
@param: :name The name of the new group
@returns: Status of request (OK if successful) and ID of the new group
@errors: GROUP_ID_ALREADY_IN_USE, INVALID_OR_MISSING_NAME_FIELD, OPERATION_FAILED

=cut
any ['post','put'] => '/cell_ids/groups/new' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V'));
	return api_error('INVALID_OR_MISSING_NAME_FIELD') if (!params->{name});

	my ($exists, $group_id, $affected, $result);

	# Check if cell group id is provided
	if (params->{id}) {
		# Check if cell group id already exists
		$exists = get_value_from_db($dbh, "SELECT 1 FROM RT_CELL_GROUPS WHERE ID=?", params->{id});
		return api_error('GROUP_ID_ALREADY_IN_USE') if ($exists);

		$group_id = params->{id};
	} else {
		# Get new cell group id (max+1)
		$group_id = get_value_from_db($dbh, "SELECT MAX(ID) FROM RT_CELL_GROUPS");
		if (defined($group_id)) {$group_id++;}
		else {$group_id = 1;}
	}

	# Insert new cell group into db
	$affected = execute_sql($dbh, "INSERT INTO RT_CELL_GROUPS (ID, NAME) VALUES (?,?)", $group_id, params->{name});
	if (defined($affected) && $affected) {
		$result = "OK";
	} else {
		return api_error('OPERATION_FAILED');
	}

	return {result => $result, group_id => $group_id};
};

=head2 post /cell_ids/fetch
This API is used to fetch all existing cell_ids.
@returns: Status of request (OK if successful)
@errors: OPERATION_FAILED

=cut
any ['get'] => '/cell_ids/fetch' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V'));
	my ($rows, $result);

	$rows = get_rows_from_db("select distinct(cell_id) from tbl_cell_id;");
	if (defined($rows)) {
		$rows=$rows->[0];
		$result = "OK";
	} else {
		return api_error('OPERATION_FAILED');
	}

	return {result => $result,cell_ids=>$rows};
};
=head2 post /cell_ids/add
This API is used to add a new Cell ID into  a group in the DB.
@param: :group_id The ID of the cell group
@param: :cell_id The cell ID value
@returns: Status of request (OK if successful)
@errors: MISSING_PARAM, GROUP_ID_NOT_FOUND, CELL_ID_ALREADY_IN_GROUP, INVALID_OR_MISSING_CELL_ID_FIELD, OPERATION_FAILED

=cut
any ['post','put'] => '/cell_ids/add' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V'));
	return api_error('MISSING_PARAM') if (!params->{group_id} || !params->{cell_id});

	my ($exists, $affected, $result);

	# Check if cell group exists
	$exists = get_value_from_db($dbh, "SELECT 1 FROM RT_CELL_GROUPS WHERE ID=?", params->{group_id});
	return api_error('GROUP_ID_NOT_FOUND') if (!$exists);

	# Check if cell id already exists in another group
	$exists = get_value_from_db($dbh, "SELECT GROUP_ID FROM TBL_CELL_ID WHERE CELL_ID=?", params->{cell_id});
	return api_error('CELL_ID_ALREADY_IN_GROUP') if ($exists && $exists != params->{group_id});

	# Insert or update cell id in db
	$affected = upsert($dbh, 'TBL_CELL_ID', {GROUP_ID => params->{group_id}, CELL_ID => params->{cell_id}});
	if (defined($affected) && $affected) {
		$result = "OK";
	} else {
		return api_error('OPERATION_FAILED');
	}

	return {result => $result};
};

=head2 post /send_message_to_roamer/:msisdn/:message_id
This API is used to send a sepcific message to a roamer
@param: :msisdn The MSISDN of the roamer
@params: :message_id The ID of the message to send
@params: lang The language of the message
@params: src_addr The message source address
@params: src_ton The message source TON
@params: src_npi The message source NPI
@params: dest_ton The message destination TON
@params: dest_npi The message destination NPI
@params: pid The message PID
@params: esm The message ESM
@params: dcs The message DCS
@returns: Status of the request (OK if successful)
@errors: MISSING_ROAMER_IDENTIFICATION, MISSING_MESSAGE_ID, NOT_ALL_MESSAGE_PARTS_SENT, NO_MESSAGE_FOUND

=cut
any ['post', 'put'] => '/send_message_to_roamer/:msisdn/:message_id' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V'));
	return api_error('MISSING_ROAMER_IDENTIFICATION') if (!params->{msisdn});
	return api_error('MISSING_MESSAGE_ID') if (!params->{message_id});

	my ($msisdn, $message_id, $lang, $src_addr, $src_ton, $src_npi, $dest_ton, $dest_npi, $is_hex, $pid, $esm, $dcs);
	my ($messages_to_send, $messages_count, $message_text, $dbid, $message_ids, $message_params);

	# Get params from request and set default values
	$msisdn = params->{msisdn};
	$message_id = params->{message_id};
	$lang = (defined(params->{lang}) ? params->{lang} : 'en');
	$src_addr = (defined(params->{src_addr}) ? params->{src_addr} : get_config_value('DEFAULT_SRC_ADDR'));
	$src_ton = (defined(params->{src_ton}) ? params->{src_ton} : get_config_value('DEFAULT_SRC_TON', 1));
	$src_npi = (defined(params->{src_npi}) ? params->{src_npi} : get_config_value('DEFAULT_SRC_NPI', 1));
	$dest_ton = (defined(params->{dest_ton}) ? params->{dest_ton} : get_config_value('DEFAULT_DEST_TON', 1));
	$dest_npi = (defined(params->{dest_npi}) ? params->{dest_npi} : get_config_value('DEFAULT_DEST_NPI', 1));
	$is_hex = (defined(params->{is_hex}) ? params->{is_hex} : 0);
	$pid = (defined(params->{pid}) ? params->{pid} : get_config_value('DEFAULT_PID', 0));
	$esm = (defined(params->{esm}) ? params->{esm} : get_config_value('DEFAULT_ESM', 0));
	$dcs = (defined(params->{dcs}) ? params->{dcs} : get_config_value('DEFAULT_DCS', 0));

	# Get message text to send
	$messages_to_send = get_rows_from_db($dbh, "SELECT SENDORDER,TEXT FROM TBL_MESSAGE_DETAILS WHERE ID=? AND LANG=? ORDER BY SENDORDER", $message_id, $lang);

	# Check if there is any text to send
	if ($messages_to_send) {
		$messages_count = scalar(@$messages_to_send);
		$message_ids = 0;

		# Go through each message
		foreach my $row (@$messages_to_send) {
			$message_text = $row->[0];
			$message_params = {
				src_addr => $src_addr, src_ton => $src_ton, src_npi => $src_npi,
				dest_ton => $dest_ton, dest_npi => $dest_npi,
				is_hex => $is_hex, pid => $pid, esm => $esm, dcs => $dcs
			};

			# Check if language is ucs2
			my $needs_ucs = $is_ucs2->{$lang};
			if ($needs_ucs) {
				$message_text = unpack('H*', utf8($message_text)->ucs2);
				$message_params->{is_hex} = 1;
				$message_params->{dcs} = 8;

				# Greek is special, capital letters fit in the GSM 3.38 but lowercase letters don't
				if ($lang eq 'el') { 
					my $gsm338 = Invigo::ucs2hex_to_gsm338($message_text);
					if ($gsm338) {
						$message_text = $gsm338;
						$message_params->{dcs} = 0;
					}
				}
			}

			# Send message
			$dbid = Invigo::send_sms_mt($dbh, $msisdn, $message_text, $message_params);
			$message_ids++ if ($dbid);
		}

		# Check if sent all messages
		return api_error('NOT_ALL_MESSAGE_PARTS_SENT') if ($messages_count != $message_ids);
	} else {
		return api_error('NO_MESSAGE_FOUND');
	}

	return {result => "OK"};
};

=head2 post /resend_message/:msisdn
Thi API is used to request resending the welcome message to a roamer according to its current location.
@param: :msisdn The MSISDN of the roamer
@returns: Status of request (OK if successful)
@errors: MISSING_ROAMER_IDENTIFICATION, ROAMER_NOT_DETECTED, OPERATION_FAILED

=cut
any ['post','put'] => '/resend_message/:msisdn' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V'));
	my $msisdn = clean_msisdn(params->{msisdn});
	log_debug("Resending message for $msisdn");
	
	return api_error('MISSING_ROAMER_IDENTIFICATION') unless $msisdn;
	my $current_roamer_info = [];
	my $roamer_type ;
	my $type_case_map = {'O' => 'bonvoyage', 'I' => 'welcome', 'L' => 'localwelcome', 'N' => 'localwelcome'};
	my $roamer = $config->{smsw_backend}->get_roamer_info_for_resend($dbh,$msisdn) || {};
	return api_error('ROAMER_NOT_DETECTED') unless scalar keys %$roamer > 0;
	#At this point we are sure the this subscriber is actually still roaming => resend messages
	$roamer->{state} = 'V';
	$roamer->{map_message} = 'ROAM';
	$roamer->{mismatch} = 0;
	$roamer->{event_date} = Invigo::get_date_time_str({date_separator => '-', time_separator => ':', date_time_separator => ' '});
	$roamer->{related_mcc} = $roamer_type =~ /(O|N)/ ? $roamer->{visited_mcc} : $roamer->{home_mcc};
	$roamer->{related_hni} = $roamer_type =~ /(O|N)/ ? $roamer->{visited_mcc} : $roamer->{home_hni};
	$roamer->{roamer_case} = $type_case_map->{ $roamer_type };
	$config->{smsw_backend}->generate_event_id($roamer) if $config->{smsw_backend} && $config->{smsw_backend}->can_do('generate_event_id');
	#resend the messages 
	my $sms_sent = prepare_and_send_message ($dbh, $roamer);
	$config->{smsw_backend}->update_sms_counter($dbh, $sms_sent, $roamer) if $sms_sent && backend_can_do('update_sms_counter');
	
	return {result => "No Messages Sent", SmsSent => $sms_sent} unless $sms_sent;

	return {result => "OK", SmsSent => $sms_sent};
};

=head2 post /update_subscriber_info/:msisdn
This API is used to insert or update a subscriber's info: Subscription, Language, Name, Nationality...
@param: :msisdn The MSISDN of the subscriber
@param: subscription The ID of the subscriber's subscription
@param: language The ID of the subscriber's language
@param: name The subscriber's name
@param: nationality The subscriber's nationality
@returns: Status of request (OK if successful)
@errors: MISSING_SUBSCRIBER_IDENTIFICATION, INVALID_SUBSCRIPTION_ID, INVALID_LANGUAGE_ID, INVALID_NATIONALITY, OPERATION_FAILED

=cut
any ['post', 'put'] => '/update_subscriber_info/:msisdn' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V'));
	return api_error('MISSING_SUBSCRIBER_IDENTIFICATION') if (!params->{msisdn});

	my ($subscription_id, $language_id, $name, $nationality);
	my ($sql_params, $exists, $res);

	# Get params from request
	$subscription_id = params->{subscription};
	$language_id = params->{language};
	$name = params->{name};
	$nationality = params->{nationality};

	# Check if subscription id is valid
	if ($subscription_id) {
		$exists = get_value_from_db($dbh, "SELECT 1 FROM RT_SUBSCRIPTION WHERE ID=?", $subscription_id);
		return api_error('INVALID_SUBSCRIPTION_ID') if (!$exists);

		$sql_params->{SUBSCRIPTION_ID} = $subscription_id;
	}

	# Check if language id is valid
	if ($language_id) {
		$exists = get_value_from_db($dbh, "SELECT 1 FROM RT_LANGUAGE WHERE ID=?", $language_id);
		return api_error('INVALID_LANGUAGE_ID') if (!$exists);

		$sql_params->{LANGUAGE_ID} = $language_id;
	}

	# Check if nationality is valid
	if ($nationality) {
		$exists = get_value_from_db($dbh, "SELECT 1 FROM RT_NATIONALITY WHERE ID=?", $nationality);
		return api_error('INVALID_NATIONALITY') if (!$exists);

		$sql_params->{NATIONALITY} = $nationality;
	}

	$sql_params->{NAME} = $name if ($name);

	# Update or insert subscribers info in db
	$res = upsert($dbh, "TBL_SUBSCRIBERS_INFO", {MSISDN => params->{msisdn}}, $sql_params);
	return api_error('OPERATION_FAILED') if (!$res);

	return {result => "OK"};
};

=head2 get /get_messages_for_roamer/:msisdn
This API is used to retrieve the messages to be send to a roamer based on their MSISDN (in all languages).
@param: :msisdn The MSISDN of the roamer
@param: request_id The ID of the request sent to IVR
@param: message_type The type of messages to return (all/tariff) (default 'all')
@param: language The language of the messages to return (default 'all')
@returns: Message texts to be sent
@errors: MISSING_ROAMER_IDENTIFICATION, ROAMER_NOT_ROAMING, NO_MSG_FOUND

=cut
get '/get_messages_for_roamer/:msisdn' => sub {

};

get '/smsw_tables/get_related_table/:table_name' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V'));
	my ($table, $related_values);

	# Get related tables data from predefined variables ($tables_with_custom_operations)
	$table = params->{table_name};
	$related_values = $tables_with_custom_operations->{$table};
	$related_values = $related_values->{params->{extra_option}} if (defined(params->{extra_option}));

	return {result => {related_table => $related_values->{'table'}, main_column => $related_values->{'main_column'}, related_column => $related_values->{'related_column'}}};
};


=head2 get /mobile_networks/data
This API is used to retrieve the countries data
@returns: Returns Object containing all configs and data needed to start mobile_networks properly 
=cut

get '/mobile_networks/data' => sub { 
	
	return api_error('ACTION_NOT_ALLOWED') unless (has_access('ACCESS_API','V') || has_access("/mobile_networks"));
	my $data= {
		country_pane => {
			pane_open => (lc $config->{DEFAULT_MOBILE_NETWORK_PANE} eq 'countries') || 0,
			foreign_language => (defined $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && defined $config->{COUNTRIES_FOREIGN_LANGUAGE} && $config->{COUNTRIES_FOREIGN_LANGUAGE}) ? $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} : 0,
			info => {
				columns => $config->{COUNTRIES_TABLE_INFO}->{COLUMNS},
				sort_columns =>  $config->{COUNTRIES_TABLE_INFO}->{SORT_COLUMNS}
			},
			max_country_code_length=>$config->{max_country_code_length}
		},
		operator_pane => {
			pane_open => (lc $config->{DEFAULT_MOBILE_NETWORK_PANE} eq 'operators') || 0,
			foreign_language => (defined $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && defined $config->{COUNTRIES_FOREIGN_LANGUAGE} && $config->{COUNTRIES_FOREIGN_LANGUAGE}) ? $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} : 0,
			prefered_operators=> $config->{prefered_operators_enabled},
			info => {
				columns => 	get_operator_columns()
			}
		},
		roaming_hub_pane => {
			enable_agt_prefix=>$config->{enable_agt_prefix},
			pane_open => (lc $config->{DEFAULT_MOBILE_NETWORK_PANE} eq 'roaming_hub') || 0,
			info => {
				columns => [
							{ID=>"ID",HIDE=>1},
							{ID=>"VLR Address", WIDTH=>135},
							{ID=>"Associated MCC",WIDTH=>100},
							{ID=>"Associated HNI", WIDTH=>150},
							{ID=>"Operator And Country Name"}
						]
			}
		},
		mcc_hni=>$cache->{mcc_hni_operators},	
	};
	
	$data->{countries} = $cache->{interface_cache}->{countries} if $countries_changed;
	$data->{operators} = $cache->{interface_cache}->{operators} if $operators_changed;
	
	return $data;
};

=head2 get /mobile_networks/:pane_name/rows
This API is used to retrive the countries 
@returns: Returns rows of data in 2-dimensional array for the respective pane
=cut

get '/mobile_networks/:pane_name/rows' => sub {

	return api_error('ACTION_NOT_ALLOWED') unless (has_access('ACCESS_API','V') || has_access("/mobile_networks"));
	return api_error('ROAMING_HUB_NOT_ENABLED') if (!$config->{enable_agt_prefix} && params->{pane_name} eq 'roaming_hub');
	
	my @params = ();
	my ($rows, $query);

	if(params->{pane_name} eq 'countries'){
		
		my $country_codes = {};
		my $country_code_rows = get_rows_from_db($dbh, 'SELECT * FROM RT_COUNTRY_CODES');
		
		$query = "select c.mcc, c.name, n.mcc IS NOT NULL, COALESCE(n.coverage_overlap,0) as coverage_overlap FROM rt_countries c LEFT JOIN tbl_neighbours n ON c.mcc = n.mcc WHERE c.name NOT LIKE '%DO_NOT_USE' AND c.name NOT LIKE '%DOT_NO_TUSE';"  unless defined $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && defined $config->{COUNTRIES_FOREIGN_LANGUAGE} && $config->{COUNTRIES_FOREIGN_LANGUAGE};

		$query = "select c.mcc, c.name, COALESCE(cn.name,'') as foreign_name, n.mcc IS NOT NULL, COALESCE(n.coverage_overlap,0) as coverage_overlap FROM rt_countries c LEFT JOIN tbl_neighbours n ON c.mcc = n.mcc LEFT JOIN tbl_countries_names cn ON cn.mcc = c.mcc AND cn.lang = ? WHERE c.name NOT LIKE '%DO_NOT_USE' AND c.name NOT LIKE '%DOT_NO_TUSE';" if defined $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && defined $config->{COUNTRIES_FOREIGN_LANGUAGE} && $config->{COUNTRIES_FOREIGN_LANGUAGE};

		push @params ,$config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} if defined $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && defined $config->{COUNTRIES_FOREIGN_LANGUAGE} && $config->{COUNTRIES_FOREIGN_LANGUAGE};
		$rows =	get_rows_from_db($dbh,$query,@params);

		foreach my $country_code (@$country_code_rows){
			$country_codes->{${$country_code}[1]} = [] if !$country_codes->{${$country_code}[1]};
			push @{$country_codes->{${$country_code}[1]}}, ${$country_code}[0];
		}

		foreach my $row (@$rows){ 
			push @$row , $country_codes->{$row->[0]} if defined $country_codes->{$row->[0]};
			push @$row , [] unless defined $country_codes->{$row->[0]};	

		}

		return $rows;

	} elsif(params->{pane_name} eq 'roaming_hub' ){

		$query = "select v.agt_prefix, v.mcc , v.hni, CONCAT(o.name,' - ',c.name) FROM tbl_agt v INNER JOIN rt_countries c ON v.mcc = c.mcc INNER JOIN rt_operators o ON v.hni = o.hni;";
		$rows = get_rows_from_db($dbh,$query);
		return $rows;

	} elsif(params->{pane_name} eq 'operators' ){

		my $operator_numbers =cache_tbl_operator_numbers_by_hni($dbh);

		# Check if foreign language is enabled, if so, fetch language specific operator names
		if(defined $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && defined $config->{COUNTRIES_FOREIGN_LANGUAGE} && $config->{COUNTRIES_FOREIGN_LANGUAGE}){
			$query = " select o.hni, c.mcc ,c.name, o.name, coalesce(opn.name,'') from rt_operators o inner join rt_countries c on o.mcc = c.mcc left join tbl_operators_names opn on opn.hni = o.hni and opn.lang = ? WHERE ((o.name NOT LIKE '%DO_NOT_USE' AND o.name NOT LIKE '%DOT_NO_TUSE') AND (c.name NOT LIKE '%DO_NOT_USE' AND c.name NOT LIKE '%DOT_NO_TUSE')) group by o.hni,c.name,c.mcc,opn.name";
			
			$query = "select o.hni, c.mcc ,c.name, o.name, coalesce(opn.name,''), COALESCE(count(p.hni),0) from rt_operators o inner join rt_countries c on o.mcc = c.mcc left join tbl_operators_names opn on opn.hni = o.hni and opn.lang = ? left join tbl_prefered_operators p on o.hni = p.hni and o.mcc = p.mcc WHERE ((o.name NOT LIKE '%DO_NOT_USE' AND o.name NOT LIKE '%DOT_NO_TUSE') AND (c.name NOT LIKE '%DO_NOT_USE' AND c.name NOT LIKE '%DOT_NO_TUSE')) group by o.hni,c.mcc,opn.name"  if $config->{prefered_operators_enabled};
		}else{
			$query = "select o.hni, c.mcc ,c.name, o.name from rt_operators o inner join rt_countries c on o.mcc = c.mcc WHERE ((o.name NOT LIKE '%DO_NOT_USE' AND o.name NOT LIKE '%DOT_NO_TUSE') AND (c.name NOT LIKE '%DO_NOT_USE' AND c.name NOT LIKE '%DOT_NO_TUSE')) group by o.hni,c.name,c.mcc";
			
			$query = "select o.hni, c.mcc ,c.name, o.name,COALESCE(count(p.hni),0) from rt_operators o inner join rt_countries c on o.mcc = c.mcc left join tbl_prefered_operators p on o.hni = p.hni and o.mcc = p.mcc WHERE ((o.name NOT LIKE '%DO_NOT_USE' AND o.name NOT LIKE '%DOT_NO_TUSE') AND (c.name NOT LIKE '%DO_NOT_USE' AND c.name NOT LIKE '%DOT_NO_TUSE')) group by o.hni,c.mcc"  if $config->{prefered_operators_enabled};
		}

		push @params ,$config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} if defined $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && defined $config->{COUNTRIES_FOREIGN_LANGUAGE} && $config->{COUNTRIES_FOREIGN_LANGUAGE};

		$rows =	get_rows_from_db($dbh,$query,@params);

		foreach my $row (@{$rows}){
			
			push @{$row}, $operator_numbers->{$row->[0]} ? $operator_numbers->{$row->[0]} : {PREFIXES=>[],MSISDN_RANGES=>[]};
		}

		return $rows;

	}
	
	return api_error('PANE_NOT_FOUND');

};

=head2 post /mobile_networks/:pane/add
This API is used to add new country/operator/roaming_hub records to the database
@Request Body: Must send Object containing needed informations to properly Add the new records
@Pane Parameter: values allowed are 'countries', 'operators', 'roaming_hub' (Roaming Hub is Optional and returns error if not enabled)
@Returns: OK if succeed
@Errors: ACTION_NOT_ALLOWED, BAD_PARAMETERS, '{{PANE}} Update Failed', 'New {{PANE}} Add failed' and ROAMING_HUB_NOT_ENABLED (if roaming_hub not enabled by config)
'Country Update Failed' could be caused if we're trying to update the MCC of a country that's already referenced in other tables
=cut

post '/mobile_networks/:pane/add' => sub {
	
	my $res;
	my $sqls_to_execute = [];
	my $pane = params->{pane};
	my $params = params;

	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'E') || has_access("/smswelcome/mobile_networks/$pane",'E'));
	return api_error("PANE_DOESNT_EXIST") unless $pane =~ /^(operators|countries|roaming_hub)$/;
	
	if($pane eq 'countries'){
		
		return api_error('BAD_PARAMETERS') unless defined $params->{mcc} && defined $params->{country_name} && defined $params->{neighbour_coverage} && defined $params->{country_code};

		if($params->{old_id} && $params->{old_id} ne '' && $params->{old_id} eq $params->{mcc}){
			#Check if MCC country code is already in the database in the case of update
			foreach my $country (@{$params->{country_code}}) {
					if ($cache->{rt_country_codes}->{$country->{code}}) {
						return api_error("Country Code $country->{code} Already Exists!");
    				}
				}

			my $has_foreign_name_record = get_value_from_db($dbh,"SELECT COUNT(*) FROM TBL_COUNTRIES_NAMES WHERE mcc = ? AND lang = ?",$params->{mcc},$config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR});

			push @$sqls_to_execute , ["UPDATE RT_COUNTRIES set mcc = ?, name = ? WHERE mcc = ?",($params->{mcc},$params->{country_name},$params->{old_id})];
			push @$sqls_to_execute , ["DELETE FROM TBL_NEIGHBOURS WHERE mcc = ?",$params->{old_id}];
			if ($params->{neighbour_coverage}){
				push @$sqls_to_execute , ["INSERT INTO TBL_NEIGHBOURS(mcc, coverage_overlap) VALUES (?,?)",($params->{mcc},$params->{coverage_overlap}?1:0)] if $params->{neighbour_coverage};
			}
			push @$sqls_to_execute , ["DELETE FROM RT_COUNTRY_CODES WHERE mcc = ?",$params->{old_id}];
			push @$sqls_to_execute , ["INSERT INTO RT_COUNTRY_CODES(code,mcc) VALUES ".join(",", map {'(?,?)'} @{$params->{country_code}})."",(map {$_->{code},$params->{mcc}} @{$params->{country_code}})];
			push @$sqls_to_execute , ["UPDATE TBL_COUNTRIES_NAMES set mcc = ?, lang = ?, name = ? WHERE mcc = ?",($params->{mcc},$config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR},$params->{country_foreign_language_name},$params->{old_id})]  if defined $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && $has_foreign_name_record;
			push @$sqls_to_execute , ["INSERT INTO TBL_COUNTRIES_NAMES(mcc,lang,name) VALUES (?,?,?)",($params->{mcc},$config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR},$params->{country_foreign_language_name})]  if defined $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && !$has_foreign_name_record;

		} else{
			#In case of new country add

			if ($params->{old_id} && $params->{old_id} ne '' && $params->{old_id} ne $params->{mcc}){

				push @$sqls_to_execute , ["DELETE FROM RT_COUNTRY_CODES WHERE mcc = ?",$params->{old_id}];
				push @$sqls_to_execute , ["DELETE FROM TBL_NEIGHBOURS WHERE mcc = ?",$params->{old_id}];
				push @$sqls_to_execute , ["DELETE FROM TBL_COUNTRIES_NAMES WHERE mcc = ?",$params->{old_id}] if  $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR};
				push @$sqls_to_execute , ["DELETE FROM RT_COUNTRIES WHERE mcc = ?",$params->{old_id}];

			}
			#Check if MCC is already in the database
			if($cache->{rt_countries}->{$params->{mcc}} && $cache->{rt_countries}->{$params->{mcc}}){
				return api_error("MCC Already Exists!");
			} else{
				#If MCC not in database, check  if country code is already in the database
				foreach my $country (@{$params->{country_code}}) {
					if ($cache->{rt_country_codes}->{$country->{code}}) {
						return api_error("Country Code Already Exists!");
    				}
				}
			}

			push @$sqls_to_execute , ["INSERT INTO RT_COUNTRIES(mcc,name) VALUES (?,?)",($params->{mcc},$params->{country_name})];
			push @$sqls_to_execute , ["INSERT INTO TBL_NEIGHBOURS(mcc, coverage_overlap) VALUES (?,?)",($params->{mcc},$params->{coverage_overlap}?1:0)] if $params->{neighbour_coverage};
			push @$sqls_to_execute , ["INSERT INTO RT_COUNTRY_CODES(code,mcc) VALUES ".join(",", map {'(?,?)'} @{$params->{country_code}})."",(map {$_->{code},$params->{mcc}} @{$params->{country_code}})];
			push @$sqls_to_execute , ["INSERT INTO TBL_COUNTRIES_NAMES(mcc,lang,name) VALUES (?,?,?)",($params->{mcc},$config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR},$params->{country_foreign_language_name})]  if defined $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR};

		}

		$res = Invigo::execute_sqls($dbh,@$sqls_to_execute);
		Invigo::set_db_update_time("TBL_COUNTRIES") if $res;
		return {status => "OK"} if $res;
		return api_error("Failed to Update Country") if $params->{old_id} && $params->{old_id} ne '';
		return api_error("Failed to add a New Country");

	}elsif($pane eq 'operators'){

		return api_error('BAD_PARAMETERS') unless defined $params->{hni} && defined $params->{mcc} && defined $params->{operator_name};

		if($params->{old_hni} && $params->{old_hni} ne '' && $params->{old_hni} eq $params->{hni}){

			push @$sqls_to_execute , ["UPDATE RT_OPERATORS SET MCC=?,NAME=? WHERE HNI=?",params->{mcc},params->{operator_name},params->{hni}];
			push @$sqls_to_execute , ["DELETE FROM TBL_OPERATOR_NUMBERS WHERE hni = ?", params->{old_hni}];
			push @$sqls_to_execute , ["UPDATE tbl_operators_names SET name = ? WHERE hni = ?",params->{operator_foreign_language_name},params->{hni}] if $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR};

			if ($config->{prefered_operators_enabled}){
				push @$sqls_to_execute , ["DELETE FROM TBL_PREFERED_OPERATORS WHERE hni = ?", params->{hni}]; 
				push @$sqls_to_execute , ["INSERT INTO TBL_PREFERED_OPERATORS VALUES (?,?)",params->{mcc},params->{hni}] if params->{preferred_operator}; 
			}

			if ((scalar @{params->{operator_prefixes}} gt 0 || scalar @{params->{operator_ranges}} gt 0)){
				my $query="INSERT INTO TBL_OPERATOR_NUMBERS VALUES";
				my @params=();
				my @values=();
				foreach (@{params->{operator_prefixes}}){
					next unless $_ || !defined($_);
					#Check if operator prefix already exists in the case of an update
					if ($cache->{all_operator_prefixes}->{$_} && $cache->{all_operator_prefixes}->{$_}!= $params->{hni}) {
						return api_error("Operator Prefix $_ Already Exists!");
    				}
					push @params,"(?,?,?,?)";
					push @values, params->{hni};
					push @values, $_;
					push @values, 0;
					push @values, 0;
				}

				foreach (@{params->{operator_ranges}}){
					next unless $_ || !defined($_);
					push @params,"(?,?,?,?)";
					push @values, params->{hni};
					push @values, 0;
					push @values, $_->{MSISDN_FROM};
					push @values, $_->{MSISDN_TO};
				}
				if(scalar @params >0) {
					$query .= join(' , ', @params); 
					push @$sqls_to_execute , [$query,@values];
				}
			}

		} elsif ($params->{old_hni} && $params->{old_hni} ne '' && $params->{old_hni} ne $params->{hni}){
			#Check if HNI is already in the database
			if($cache->{operator_prefixes}->{$params->{hni}}){
				return api_error("Operator HNI $params->{hni} Already Exists!");
			}
			push @$sqls_to_execute , ["DELETE FROM TBL_OPERATOR_NUMBERS WHERE hni = ?", params->{old_hni}];
			push @$sqls_to_execute , ["DELETE FROM TBL_PREFERED_OPERATORS WHERE hni = ?", params->{old_hni}]  if ($config->{prefered_operators_enabled});
			push @$sqls_to_execute , ["DELETE FROM TBL_OPERATORS_NAMES WHERE hni = ?",params->{old_hni}] if $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR};
			push @$sqls_to_execute , ["DELETE FROM RT_OPERATORS WHERE hni = ?", params->{old_hni}];

			push @$sqls_to_execute , ["INSERT INTO RT_OPERATORS VALUES (?,?,?)",params->{hni},params->{mcc},params->{operator_name}];
			push @$sqls_to_execute , ["INSERT INTO TBL_PREFERED_OPERATORS VALUES (?,?)",params->{mcc},params->{hni}] if ($config->{prefered_operators_enabled} && params->{preferred_operator}) ;
			push @$sqls_to_execute , ["INSERT INTO TBL_OPERATORS_NAMES VALUES (?,?,?)",params->{hni},$config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR},params->{operator_foreign_language_name}] if $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR};

			if (scalar @{params->{operator_prefixes}} gt 0 || scalar @{params->{operator_ranges}} gt 0){
				my $query="INSERT INTO TBL_OPERATOR_NUMBERS VALUES";
				my @params=();
				my @values=();
				foreach (@{params->{operator_prefixes}}){
					push @params,"(?,?,?,?)";
					push @values, params->{hni};
					push @values, $_;
					push @values, 0;
					push @values, 0;
				}

				foreach (@{params->{operator_ranges}}){
					push @params,"(?,?,?,?)";

					push @values, params->{hni};
					push @values, 0;
					push @values, $_->{MSISDN_FROM};
					push @values, $_->{MSISDN_TO};
				}
				$query .= join(' , ', @params); 
				push @$sqls_to_execute , [$query,@values];
			}

		}else{
			#Check if HNI is already in the database in the case of new operator
			if($cache->{operator_prefixes}->{$params->{hni}}){
				return api_error("Operator HNI Already Exists!");
			}else {
				#Check if operator prefix already exists in the case of an update
				foreach my $prefix (@{$params->{operator_prefixes}}) {
					if ($cache->{all_operator_prefixes}->{$prefix}) {
						return api_error("Operator Prefix $prefix Already Exists!");
    				}
			}
			}
			push @$sqls_to_execute , ["INSERT INTO RT_OPERATORS VALUES (?,?,?)",params->{hni},params->{mcc},params->{operator_name}];
			push @$sqls_to_execute , ["INSERT INTO TBL_PREFERED_OPERATORS VALUES (?,?)",params->{mcc},params->{hni}]  if ($config->{prefered_operators_enabled} && params->{preferred_operator});
			push @$sqls_to_execute , ["INSERT INTO TBL_OPERATORS_NAMES VALUES (?,?,?)",params->{hni},$config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR},params->{operator_foreign_language_name}] if $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR};

			params->{operator_prefixes} = [] unless defined params->{operator_prefixes};
			params->{operator_ranges} = [] unless defined params->{operator_ranges};
			
			params->{operator_ranges} = [params->{operator_ranges}] if ref(params->{operator_ranges}) ne 'ARRAY';
			params->{operator_prefixes} = [params->{operator_prefixes}] if ref(params->{operator_prefixes}) ne 'ARRAY';

			if (scalar @{params->{operator_prefixes}} gt 0 || scalar @{params->{operator_ranges}} gt 0){

				my $query="INSERT INTO TBL_OPERATOR_NUMBERS VALUES";
				my @params=();
				my @values=();
				foreach (@{params->{operator_prefixes}}){
					push @params,"(?,?,?,?)";
					push @values, params->{hni};
					push @values, $_;
					push @values, 0;
					push @values, 0;
				}

				foreach (@{params->{operator_ranges}}){
					push @params,"(?,?,?,?)";
				
					push @values, params->{hni};
					push @values, 0;
					push @values, $_->{MSISDN_FROM};
					push @values, $_->{MSISDN_TO};
				}
				$query .= join(' , ', @params); 
				push @$sqls_to_execute , [$query,@values];
			}

		}

		$res = Invigo::execute_sqls($dbh,@$sqls_to_execute);
		Invigo::set_db_update_time("TBL_OPERATORS") if $res;
		return {status => "OK"} if $res;
		return api_error("New Operator Add failed") unless $params->{old_hni} && $params->{old_hni} ne '' ;
		return api_error("Operator Update failed");

	}elsif($pane eq 'roaming_hub'){

		return api_error('ROAMING_HUB_NOT_ENABLED') if !$config->{enable_agt_prefix};
		return api_error('BAD_PARAMETERS') unless defined$params->{mcc} && defined $params->{vlr_address} && defined $params->{hni};

		my @old_params=split ",",$params->{old_id} if $params->{old_id} && defined $params->{old_id};
		$res = Invigo::execute_sql($dbh,"UPDATE tbl_agt set agt_prefix=?, hni=?, mcc=? where agt_prefix=? and hni=?", $params->{vlr_address},$params->{hni},$params->{mcc},$old_params[0],$old_params[1]) if $params->{old_id} && defined $params->{old_id};
		$res = Invigo::execute_sql($dbh,"Insert into tbl_agt values (?,?,?)", $params->{vlr_address},$params->{hni},$params->{mcc}) unless $params->{old_id} && defined $params->{old_id};

		Invigo::set_db_update_time("TBL_AGT") if $res;
		return {status => "OK"} if $res;
		return api_error("New Roaming Hub Add failed") unless $params->{old_id} && defined $params->{old_id};
		return api_error("Roaming Hub Update failed");
		
	}

};


=head2 post /mobile_networks/:pane/delete/:id/:prefix?
This API is used to delete Country,Operator or Roaming Hub records
=cut

post '/mobile_networks/:pane/delete/:id/:prefix?' => sub {

	my $res;
	my $sqls_to_execute = [];
	my $pane = params->{pane};

	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'D') || has_access("/smswelcome/mobile_networks/$pane",'D'));
	return api_error("PANE_DOESNT_EXIST") unless $pane =~ /^(operators|countries|roaming_hub)$/;

	if($pane eq 'operators'){


		push @$sqls_to_execute , ["DELETE FROM TBL_OPERATORS_NAMES WHERE hni = ?",params->{id}];
		push @$sqls_to_execute , ["DELETE FROM TBL_OPERATOR_NUMBERS WHERE hni = ?", params->{id}];
		push @$sqls_to_execute , ["DELETE FROM TBL_PREFERED_OPERATORS WHERE hni = ?",params->{id}] if ($config->{prefered_operators_enabled});
		push @$sqls_to_execute , ["DELETE FROM RT_OPERATORS WHERE hni = ?",params->{id}];

		$res = Invigo::execute_sqls($dbh,@$sqls_to_execute);

		Invigo::set_db_update_time("TBL_OPERATORS") if $res;
	}elsif($pane eq 'countries'){

		push @$sqls_to_execute , ["DELETE FROM RT_COUNTRY_CODES WHERE mcc = ?", params->{id}];
		push @$sqls_to_execute , ["DELETE FROM TBL_NEIGHBOURS WHERE mcc = ?",params->{id}];
		push @$sqls_to_execute , ["DELETE FROM TBL_COUNTRIES_NAMES WHERE mcc = ?",params->{id}] if  $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR};
		push @$sqls_to_execute , ["DELETE FROM RT_COUNTRIES WHERE mcc = ?",params->{id}];

		$res = Invigo::execute_sqls($dbh,@$sqls_to_execute);
		
		Invigo::set_db_update_time("TBL_COUNTRIES") if $res;
	}elsif($pane eq 'roaming_hub'){

		return api_error('ROAMING_HUB_NOT_ENABLED') if !$config->{enable_agt_prefix};
		$res=Invigo::execute_sql($dbh,"Delete from tbl_agt where agt_prefix=? and hni=?",params->{id},params->{prefix});
		Invigo::set_db_update_time("TBL_AGT") if $res;
	}
	
	return {status => "OK"} if $res;
	return api_error("Delete failed");

};

=head2 get /subscriber_lookup/config
This API is used to retrieve the configs for the user interface
=cut

get '/subscriber_lookup/config' => sub {

	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access("/subscribers_lookup"));

	$config->{message_delivery_status_enabled} = uc(Invigo::get_config_value('STATS_DELIVERY_REPORTS_ENABLED', 'NO'));
	$config->{message_delivery_status_enabled} = ($config->{message_delivery_status_enabled} ne 'N' && $config->{message_delivery_status_enabled} ne 'NO' && $config->{message_delivery_status_enabled} ne '0' ? 1 : 0);

	my $event_to_history = [
		{ ID => "MSISDN", WIDTH => "130"},
		{ ID => "IMSI", WIDTH => "150"},
		{ ID => "Event", WIDTH => "140" },
		{ ID => "Visited Country", WIDTH => "150"},
		{ ID => "Visited Operator", WIDTH => "155"},
		{ ID => "VLR Address", WIDTH => "133" },
		{ ID => "Date Visiting", WIDTH => "155"},
		{ ID => "Date Last Seen", WIDTH => "155"},
		{ ID => "Date Out", WIDTH => "155"}
	];

	$event_to_history = [
		{ ID => "MSISDN", WIDTH => "130"},
		{ ID => "IMSI", WIDTH => "150"},
		{ ID => "Secondary IMSI", WIDTH => "150"},
		{ ID => "Event", WIDTH => "140" },
		{ ID => "Visited Country", WIDTH => "150"},
		{ ID => "Visited Operator", WIDTH => "155"},
		{ ID => "VLR Address", WIDTH => "133" },
		{ ID => "Date Visiting", WIDTH => "155"},
		{ ID => "Date Last Seen", WIDTH => "155"},
		{ ID => "Date Out", WIDTH => "155"}
	] if $config->{manage_multi_device_imsi};

	my $sms_history = [
		{ ID => "MSISDN"},
		{ ID => "Date"},
		{ ID => "Visited Country"},
		{ ID => "Visited Operator"},
		{ ID => "Message Type"},
		{ ID => "Message ID"},
		{ ID => "Message Text"}
	];

	push @$sms_history, { ID => "Delivery Status"} if $config->{message_delivery_status_enabled};

	return {
		result => "OK",
		config => {
			delivery_status_report => $config->{message_delivery_status_enabled}
		},
		sms_history_columns => $sms_history,
		events_history_columns => $event_to_history
	};

};

=head2 get /subscriber_lookup/profile/:searchable
This API is used to retrieve the profile and data for a given msisdn
@returns: Return Object containing all roamers informations
=cut

get '/subscriber_lookup/profile/:searchable' => sub {

	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access("/subscribers_lookup"));
	return api_error('MISSING_ROAMER_IDENTIFICATION') if (!params->{searchable});

	my $order_id = 0;
	my ($roamer_information,$roamer) = {};
	my ($sql, $row, $result,$where,$type,$id,@arr_split,$is_secondary_imsi);
	my %wanted_properties = (map {$_ => 1} @{$config->{ROAMER_CURRENT_LOCATION}}, map {$_ => 1} @{$config->{ROAMER_PROFILE}}, map {$_ => 1} @{$config->{LOOKUP_INFO_HEADER_PANE}});

	$id = params->{searchable} || "NULL";

	if($config->{elastic}){
		
		my $source = "visited_operator_code,imsi,visited_country_code,msisdn,date_last_seen,date_out,date_in,vlr,roamer_type,roamer_state,home_country_code,home_operator_code";
		$source .= ",secondary_imsi" if $config->{manage_multi_device_imsi};
		my $matches= [{ match => { msisdn => $id } },{ match => {imsi => $id} }];
		push @$matches,{exists=> {field=> "secondary_imsi.".$id}} if $config->{manage_multi_device_imsi};
		my $query = { bool => { should => $matches,minimum_should_match => 1}};
		my @reqt_smsw_event = Invigo::Elastic::dump_index( $config->{smsw_current},$source,$query,undef,[{ date_last_seen => {order => 'asc'}},{ date_out => {order => 'desc'}},{ date_in => {order => 'desc'}}]);
		return api_error("ROAMER_NOT_FOUND") unless scalar @reqt_smsw_event;
		
		$row = $reqt_smsw_event[0];
		
		if(lc $row->{roamer_type} eq 'outbound' && lc $row->{roamer_state} ne 'visiting'){
			$row->{STATUS} = 'Back';
		}elsif(lc $row->{roamer_state} eq 'visiting' && !defined $row->{date_out}){
			$row->{STATUS} = 'Roaming';
		}else{
			$row->{STATUS} = 'Not Roaming';
		}

		foreach (keys %$row){
			$row->{uc $_} = $row->{$_};
			delete($row->{lc $_});
		}

		$result = "OK";
		return api_error('ROAMER_NOT_FOUND') if !defined($row);
		$is_secondary_imsi= defined($row->{SECONDARY_IMSI}->{$id}) if $config->{manage_multi_device_imsi} && defined $row->{SECONDARY_IMSI};
	}

	return api_error('CONNECTION_TO_ELASTIC_FAILED') unless $config->{elastic};
	
	$roamer->{type} = $row->{TYPE} || lc $row->{ROAMER_TYPE} eq 'outbound'? 'O':'I';
	$roamer->{imsi} = $row->{IMSI};
	$roamer->{msisdn} = $row->{MSISDN};
	$roamer->{vlr} = $row->{VIS_VLR_LOCATION} || $row->{vlr};

	@{$row->{LANGUAGE}} = get_language($dbh , $roamer); 
	$row->{VIS_OPERATOR} ||= $row->{VISITED_OPERATOR_CODE};
	$row->{VIS_COUNTRY} ||= $row->{VISITED_COUNTRY_CODE};
	$row->{HOME_OPERATOR} ||= $row->{HOME_OPERATOR_CODE};
	$row->{HOME_COUNTRY} ||= $row->{HOME_COUNTRY_CODE};
	$row->{LAST_SEEN} ||= $row->{DATE_LAST_SEEN} || $row->{DATE_OUT} ||  $row->{DATE_IN};
	$row->{LAST_SEEN} =~ tr/T/ /;
	$row->{SECONDARY_IMSI} = join ' , ', keys %{$row->{SECONDARY_IMSI}} if $config->{manage_multi_device_imsi} && defined $row->{SECONDARY_IMSI};

	#Fetch roamer's home region if column is requested and operator supports this feature
	my $roamer_local = ($config->{local_send_sms} && defined(&get_roamer_local_info) ? get_roamer_local_info($dbh,$roamer) : 0);
	$roamer_information->{HOME_REGION} = {value => $roamer_local->{home_region}, panel => []} if $roamer_local;
	$roamer_information->{VISITED_REGION} = {value => $roamer_local->{visited_region}, panel => []} if $roamer_local;
	
	$roamer->{state} = $roamer_local->{home_region}->{value} if $roamer_local;

	#Keep it here because we do need in some cases the state 
	my $profile = get_subscriber_profile($dbh,$roamer);

	for my $property (keys %$profile){
		$row->{uc $property} = $profile->{$property} if !defined $row->{uc $property};
	}

	# Map Roamer's information into a new hash
	%$roamer_information = map { $_ => { value => $row->{$_}, panel => [] } }  grep { defined $wanted_properties{lc $_} } keys %$row;
	$roamer_information->{TYPE} = { panel => [], value => ($roamer->{type} eq 'O'?'Outbound':'Inbound')};
	$roamer_information->{HOME_COUNTRY_NAME} = { panel => [], value => $cache->{rt_countries}->{$row->{HOME_COUNTRY}}};
	$roamer_information->{HOME_OPERATOR_NAME} = {value => $cache->{rt_operators}->{$row->{HOME_OPERATOR}}, panel => []};
	$roamer_information->{VIS_COUNTRY_NAME} = {value => $cache->{rt_countries}->{$row->{VIS_COUNTRY}}, panel => []};
	$roamer_information->{VIS_COUNTRY_NAME} = {value => "N/A" ,panel => []} if defined $row->{DATE_OUT};
	$roamer_information->{VIS_OPERATOR_NAME} = {value => $cache->{rt_operators}->{$row->{VIS_OPERATOR}}, panel => []};
	$roamer_information->{VIS_OPERATOR_NAME} = {value => "N/A" ,panel => []} if defined($row->{DATE_OUT});
	$roamer_information->{COUNTRY_AND_OPERATOR} = {panel => [], value => $roamer_information->{VIS_COUNTRY_NAME}->{value}." - ".$roamer_information->{VIS_OPERATOR_NAME}->{value}};

	@{$roamer_information->{LANGUAGE}->{value}} = map { $cache->{rt_language}->{$_} } @{$roamer_information->{LANGUAGE}->{value}};

	if(defined $config->{LOOKUP_INFO_HEADER_PANE}){
		foreach my $property (@{$config->{LOOKUP_INFO_HEADER_PANE}}){
			$roamer_information->{uc $property}->{order} = $order_id++ if defined $roamer_information->{uc $property} && !defined($roamer_information->{uc $property}->{order});
			push @{$roamer_information->{uc $property}->{panel}},"header_profile_info" if defined $roamer_information->{uc $property};
		}
	}
	
	if(defined $config->{ROAMER_PROFILE}){
		foreach my $property (@{$config->{ROAMER_PROFILE}}){
			$roamer_information->{uc $property}->{order} = $order_id++ if defined $roamer_information->{uc $property} && !defined($roamer_information->{uc $property}->{order});
			push @{$roamer_information->{uc $property}->{panel}},"roamer_profile" if defined $roamer_information->{uc $property};
		}
	}
	
	if(defined $config->{ROAMER_CURRENT_LOCATION}){	
		foreach my $property (@{$config->{ROAMER_CURRENT_LOCATION}}){
			$roamer_information->{uc $property}->{order} = $order_id++ if defined $roamer_information->{uc $property} && !defined($roamer_information->{uc $property}->{order});
			push @{$roamer_information->{uc $property}->{panel}},"roamer_current_location" if defined $roamer_information->{uc $property};
		}
	}

	$roamer_information->{HOME_COUNTRY} = {value => $row->{HOME_COUNTRY} ,panel => [], order => 999} unless defined $roamer_information->{HOME_COUNTRY};
	$roamer_information->{VIS_COUNTRY} = {value => $row->{VIS_COUNTRY},panel => [], order => 999} unless defined $roamer_information->{VIS_COUNTRY};
	$roamer_information->{VIS_COUNTRY} = {value => "N/A" ,panel => [], order => 999} if defined($row->{DATE_OUT});
	$roamer_information->{VIS_OPERATOR} = {value => "N/A" ,panel => [], order => 999} if defined($row->{DATE_OUT});

	my $res={result => $result, info => $roamer_information};
	if($config->{manage_multi_device_imsi} && $is_secondary_imsi){
		$res->{primary_imsi}=$roamer_information->{IMSI}->{value};
	}

	return $res;
};

=head2 post /subscriber_lookup/history/:subscriber
This API is used to retrieve the history of records catched for a given subscriber
@returns: Return Array of arrays containing all the records for a given subscriber
=cut

post '/subscriber_lookup/history/:subscriber' => sub {

	my ($sql, $rows, $result, @reqt_smsw_event);
	my ($id,$msisdn_roamer_type,$imsi_roamer_type,$where);
	return api_error('MISSING_ROAMER_IDENTIFICATION') unless params->{subscriber};
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access("/subscribers_lookup"));
	
	$id = params->{subscriber} || "NULL";
	
	if($config->{elastic}){
		my $source = "msisdn,imsi,vlr,event_name,visited_country_name,visited_operator_name,date_in,date_last_sms,date_out";
		$source .= ",secondary_imsi" if $config->{manage_multi_device_imsi};
		my $query;

		if ((!params->{primary_imsi}) && ($config->{manage_multi_device_imsi} && !($config->{InvigoCache}->exists('tbl_multi_device_imsi_detections',$id)))){
			my $count = get_value_from_db("SELECT count(*) FROM TBL_MULTI_DEVICE_IMSI_DETECTIONS WHERE PRIMARY_IMSI=?",$id);
			if (!$count){
				log_debug("Searching for primary imsi");
				my @imsis = Invigo::Elastic::dump_index($config->{smsw_current},"imsi",{exists=> {field=> "secondary_imsi.".$id}},undef,[{ date_last_seen => {order => 'asc'}},{ date_out => {order => 'desc'}},{ date_in => {order => 'desc'}}]);
				$query= {"bool"=>{"should"=> [{ "match"=> { "imsi"=>$imsis[0]->{imsi}} }],minimum_should_match => 1}};
			}
		}
		$query ||= {"bool"=>{"should"=> [{ "match"=>{"msisdn"=>$id}},{ "match"=> { "imsi"=>$id} }],minimum_should_match => 1}};
		$query = {"bool"=>{"should"=> [{ "match"=> { "imsi"=>params->{primary_imsi}} }]}} if params->{primary_imsi};
		my $order = [{date_last_sms => {order => "desc"}},{ date_in => {order => 'desc' }}];
		@reqt_smsw_event = Invigo::Elastic::dump_index($config->{smsw_trips},$source,$query,undef,$order);
		return api_error('ROAMER_NOT_FOUND') unless scalar @reqt_smsw_event;
	}
	return api_error('CONNECTION_TO_ELASTIC_FAILED') unless $config->{elastic};

	@reqt_smsw_event = map {[ $_->{msisdn}, $_->{imsi}, $_->{event_name}, $_->{visited_country_name}, $_->{visited_operator_name}, $_->{vlr},format_elastic_date_for_display($_->{date_in}), format_elastic_date_for_display($_->{date_last_sms}),format_elastic_date_for_display($_->{date_out}), 0]} @reqt_smsw_event unless $config->{manage_multi_device_imsi};
	@reqt_smsw_event = map {[ $_->{msisdn}, $_->{imsi}, $_->{secondary_imsi} || 'N/A', $_->{event_name}, $_->{visited_country_name}, $_->{visited_operator_name}, $_->{vlr},format_elastic_date_for_display($_->{date_in}), format_elastic_date_for_display($_->{date_last_sms}),format_elastic_date_for_display($_->{date_out}), 0]} @reqt_smsw_event if $config->{manage_multi_device_imsi};
	return { result => "OK" , history => \@reqt_smsw_event };

};

=head2 post /subscriber_lookup/sms/:subscriber
This API is used to retrieve the sms history and details for a given subscriber
@returns: Returns Array of arrays containing all the sms records for the given subscriber
=cut

post '/subscriber_lookup/sms/:subscriber' => sub {

	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access("/subscribers_lookup"));

	my ($id,$roamer_type,$where);
	$config->{message_delivery_status_enabled} = uc(Invigo::get_config_value('STATS_DELIVERY_REPORTS_ENABLED', 'NO'));
	$config->{message_delivery_status_enabled} = ($config->{message_delivery_status_enabled} ne 'N' && $config->{message_delivery_status_enabled} ne 'NO' && $config->{message_delivery_status_enabled} ne '0' ? 1 : 0);

	$id = params->{subscriber} || return api_error('MISSING_ROAMER_IDENTIFICATION');
	$roamer_type = Invigo::get_value_from_db($dbh,"select type from tbl_roamers where msisdn = ? ORDER BY date_last DESC LIMIT 1", $id);
	$where = $roamer_type ? "MSISDN = ?":"IMSI = ?" unless $config->{elastic};
	$roamer_type ||= Invigo::get_value_from_db($dbh,"select type from tbl_roamers where imsi = ? ORDER BY date_last DESC LIMIT 1", $id);
	
	return api_error("CONFIGURATION_ERROR") unless $config->{elastic};
	# return api_error('ROAMER_NOT_FOUND') unless $roamer_type;

	return {result => "OK" , history => retrieve_subscriber_sms_history($id)} if defined(&retrieve_subscriber_sms_history);

	# TODO: Implement Delivery Status
	my $source = "msisdn,event_date,visited_country_name,language,visited_operator_name,message_type,message_id,message_text";
	my $query;
	if ((!params->{primary_imsi}) && $config->{manage_multi_device_imsi} && !($config->{InvigoCache}->exists('tbl_multi_device_imsi_detections',$id))){
		my $count = get_value_from_db("SELECT count(*) FROM TBL_MULTI_DEVICE_IMSI_DETECTIONS WHERE PRIMARY_IMSI=?",$id);
		if (!$count){
			log_debug("Searching for primary imsi");
			my @imsis = Invigo::Elastic::dump_index($config->{smsw_current},"imsi",{exists=> {field=> "secondary_imsi.".$id}},undef,[{ date_last_seen => {order => 'asc'}},{ date_out => {order => 'desc'}},{ date_in => {order => 'desc'}}]);
			$query= {"bool"=>{"should"=> [{ "match"=> { "imsi"=>$imsis[0]->{imsi}} }],minimum_should_match => 1}};
		}
	}
	$query ||= {"bool"=>{"should"=> [{ "match"=>{"msisdn"=>$id}},{ "match"=> { "imsi"=>$id} }]}};
	$query = {"bool"=>{"should"=> [{ "match"=> { "imsi"=>params->{primary_imsi}} }]}} if params->{primary_imsi};
	my $order = [{ event_date => {order => 'desc'}}];
	my @sms_history = Invigo::Elastic::dump_index($config->{smsw_sms_events},$source,$query,undef,$order);

	return {result => "OK", history => []} unless scalar @sms_history;

	foreach (@sms_history){
		$_->{message_text} = Invigo::ucs2_with_vars_to_utf_8_with_vars($_->{message_text}) if defined $_->{language} && $is_ucs2->{$_->{language}};
		utf8::decode($_->{message_text});
	}

	@sms_history = map {[ $_->{msisdn}, format_elastic_date_for_display($_->{event_date}), $_->{visited_country_name}, $_->{visited_operator_name}, $_->{message_type}, $_->{message_id},$_->{message_text} ,undef]} @sms_history;
	return {result => "OK", history => \@sms_history};

};

=head2 get /subscriber_lookup/stats/:subscriber
This API is used to retrieve the statistics for a given subscriber
@returns: Returns object filled with the statistics for the given subscriber (tbl_statistics equivalent)
=cut

get '/subscriber_lookup/stats/:subscriber' => sub {

	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access("/subscribers_lookup"));

	my $stats=$config->{smsw_backend}->prepare_roamer_stats(params->{subscriber});
	return api_error('ROAMER_NOT_FOUND') unless scalar keys %$stats;
	
	return {result => "OK", stats => $stats};

};


sub fetch_groups_by_category {
	my $fetch_groups_by_category={};
	foreach my $key (keys %{$category_to_groups}) {
		my $v = $category_to_groups->{$key};
		my $cached = $cache->{CRITERIA_TYPE_ID}->{$v} || '';
		$fetch_groups_by_category->{$key}= $cache->{CRITERIA_TYPE_GROUP_ID}->{$cached};
	}
	return $fetch_groups_by_category;
}

sub fetch_bulk_from_group {
	my ($category,$group_id) = @_;

	return unless $category_to_groups->{lc $category};
	return keys %{$cache->{LISTS}->{$group_id}->{$cache->{CRITERIA_TYPE_ID}->{$category_to_groups->{lc $category}}}};
}

sub validate_blacklist_params { #TODO Maybe futurewise this includes all the validation logic for blacklist

	my ($params)=@_;

	return 0 if(exists $params->{msisdn} && $params->{msisdn} !~ /^[\d]+$/);	
	return operator_validate_blacklist_params($params) if defined &operator_validate_blacklist_params;
	return 1;
}

sub map_messages_to_roamer_types {

	my $map = {};
	my ($roamer_types,$events) = @_;

	my $events_to_roamers_type_map = {
		inbound => {
			welcome => 1,
			winback => 1,
			welcomeback => 1,
			thank_you => 1
		},
		outbound => {
			bon_voyage => 1,
			outbound_back => 1,
		},
		local => {
			local_back => 1,
			local_welcome => 1
		},
		national => {
			national_back => 1,
			national_welcome => 1
		},
		all=>{
			all=>1
		}
	};

	foreach my $roamer_type (@$roamer_types){
		$map->{$roamer_type} = [];
		foreach my $event (@$events){
			push @{$map->{$roamer_type}},$event if $events_to_roamers_type_map->{$roamer_type}->{$event};
		}
	}

	return $map;

}

sub map_message_categories_to_roamer_types {

	my $map = {};
	my ($roamer_types,$events) = @_;

	my $events_to_roamers_type_map = {
		inbound => {
			VIP=>1,
			Operators=>1,
			Countries=>1,
			Default=>1,
			Region=>1,
			Cell_Groups=>1,
			Group=>1,
			Generic=>1
		},
		outbound => {
			VIP=>1,
			Operators=>1,
			Countries=>1,
			Default=>1,
			Cell_Groups=>1,
			Group=>1,
			Generic=>1
		},
		local => {
			VIP=>1,
			Default=>1,
			Group=>1,
			Generic=>1
		},
		national => {
			VIP=>1,
			Default=>1,
			Operators=>1,
			Group=>1,
			Generic=>1
		}
	};

	foreach my $roamer_type (@$roamer_types){
		$map->{$roamer_type} = [];
		foreach my $event (@$events){
				next if ($event eq 'Cell_Groups' && $roamer_type eq 'outbound' && !$config->{manage_cell_id_out}) || ($event eq 'Cell_Groups' && $roamer_type eq 'inbound' && !$config->{manage_cell_id_in});
				push @{$map->{$roamer_type}},$event if $events_to_roamers_type_map->{$roamer_type}->{$event};
		}
	}

	return $map;

}

sub get_pane_columns {
	
	my @roamer_types = @_;
	my $pane_columns = {}; 
	my $available_panes = {};

	$available_panes = {
		inbound => [
			{ ID => "ID", HIDE=>1 },
			{ ID => "Message ID", WIDTH => 100},
			{ ID => "Title", WIDTH => 180 },
			{ ID => "Message Type", WIDTH => 180 },
			{ ID => "Home Network", WIDTH => 180 },
			{ ID => "Visited Network or Location", WIDTH => 280 }
		],
		national => [
			{ ID => "ID", HIDE=>1 },
			{ ID => "Message ID", WIDTH => 100},
			{ ID => "Title", WIDTH => 180 },
			{ ID => "Message Type", WIDTH => 180 },
			{ ID => "Roamer Details", WIDTH => 220},
			{ ID => "Visited Network or Location", WIDTH => 280 },
		],
		local => [
			{ ID => "ID", HIDE => 1 },
			{ ID => "Message ID", WIDTH => 100},
			{ ID => "Title" , WIDTH => 180 },
			{ ID => "Message Type", WIDTH => 180},
			{ ID => "Roamer Details", WIDTH => 220},
			{ ID => "Visited Network or Location", WIDTH => 280 },
		]
	};
	
	if ($config->{message_details_products} || $config->{message_details_mvno}){
		$available_panes->{outbound} = [
			{ ID => "ID",HIDE=>1}, 
			{ ID => "Message ID", WIDTH => 100}, 
			{ ID => "Title", WIDTH => 180}, 
			{ ID => "Message Type", WIDTH => 180}, 
			{ ID => "Message Category", WIDTH => 180}, 
			{ ID => "Visited Network or Location", WIDTH => 280},
		];
	}else{
		$available_panes->{outbound} = [
			{ ID => "ID",HIDE=>1},
			{ ID => "Message ID", WIDTH => 100},
			{ ID => "Title", WIDTH => 180},
			{ ID => "Message Type", WIDTH => 180},
			{ ID => "Roamer Details", WIDTH => 220},
			{ ID => "Visited Network or Location", WIDTH => 280},
		];
	}

	foreach my $type (@roamer_types){
		$pane_columns->{$type} = [];
		$pane_columns->{$type} = $available_panes->{$type} if defined $available_panes->{$type} && $available_panes->{$type};
	}

	return $pane_columns;

}
############################################################################
########################## - Subcription Lookup - ##########################
############################################################################

=head2 get /subscription_lookup/config
This API is used to retrieve the configs for the user interface for Subscription Lookup page
=cut

get '/subscription_lookup/config' => sub {

	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access("/subscription_lookup"));


	my $column_names = [
		{ ID => "Subscription ID", WIDTH => 180 },
		{ID => "Subscription Name", WIDTH => 360},
		{ ID => "IMSI", WIDTH => 360}
	];

	my $subscriptions = Invigo::get_rows_from_db("SELECT id,name FROM rt_subscription");

	return {
		result => "OK",
		columns => $column_names,
		subscriptions => $subscriptions
	};

};

=head2 post /subscription_lookup/imsis
This API is used to fetch IMSI - Subscription information from DB table
=cut

post '/subscription_lookup/imsis' => sub {

	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access("/subscription_lookup"));

	if (defined params->{filename}){
		return api_error("Wrong File format") unless (params->{filename}=~ m/\.txt$/ || params->{filename}=~ m/\.csv$/);
		my $file_path=$config->{UPLOAD_FOLDER}."/".params->{file};
		open(my $uploadfile, "<", $file_path) or return api_error("FILE_NOT_FOUND");
		local $/;
		my @records;
		if(params->{filename}=~ m/\.csv$/){
		 	@records  = split("\n",<$uploadfile>);
		 	map { s/\s$//g } @records;
		}else{
			@records  = split(/[, \n]/,<$uploadfile>);
			map { s/\s$//g } @records;
		}
		if(scalar grep(!/^(\d+|(\d+)-(\d+))$/,@records) > 0){
			unlink($file_path);
			return api_error("file is not formatted correctly");
		}
		params->{imsis}=\@records;
		unlink($file_path);
	}

	my @imsis = @{params->{imsis}};
	return api_error("List of imsis is empty.") if scalar @imsis eq 0;

	my %unique_imsis;
	my @ranges = ();


	#Check if submitted values include ranges, process the imsis, remove duplicates
	foreach my $entry (@imsis){
		#Check if it's a range or not
		if ($entry =~ /(\d+)-(\d+)/) {

			my ($start, $end) = ($1, $2);


			#Add IMSIs as keys
			push @ranges,[$start, $end];
    	}elsif ($entry =~ /(\d+)/) {
			#Add IMSIs as keys
			$unique_imsis{$1} = 1;
    	}
	}

	#Add IMSI keys to array
	my @imsis_to_lookup = keys %unique_imsis;
	
	my $individual_query =	"SELECT rt.id,rt.name,sg.imsi FROM rt_subscription rt JOIN tbl_subscribers_groups sg ON rt.id = sg.subscription_id WHERE sg.imsi IN (".join(',',map{'?'} @imsis_to_lookup).")";
	my $range_query = "SELECT rt.id,rt.name,sg.imsi FROM rt_subscription rt JOIN tbl_subscribers_groups sg ON rt.id = sg.subscription_id WHERE ".join('OR', map{'sg.imsi BETWEEN ? AND ?'} @ranges);
	
	my @range_imsis = map {@$_} @ranges;
	my @final_imsis;
	if(scalar @range_imsis > 0 && scalar @imsis_to_lookup > 0){
		my $db_imsis = Invigo::get_rows_from_db($individual_query,@imsis_to_lookup);
		my $db_range_imsis = Invigo::get_rows_from_db($range_query,@range_imsis);

		#Merge the two arrays
		@final_imsis = (@$db_imsis, @$db_range_imsis);

	}elsif(scalar @range_imsis > 0){
		
		my $db_range_imsis = Invigo::get_rows_from_db($range_query,@range_imsis);
		@final_imsis = @$db_range_imsis;
	}elsif(scalar @imsis_to_lookup > 0){
		my $db_imsis = Invigo::get_rows_from_db($individual_query,@imsis_to_lookup);
		@final_imsis = @$db_imsis;
	}


	#Remove duplicates
	my %final_imsis_hash = map { $_->[2] => $_ } @final_imsis;

	my @final_imsis_unique = values %final_imsis_hash;
	return {
		result => "OK",
		imsis => \@final_imsis_unique};
};

=head2 post /subscription_lookup/delete/:subscription_id/:imsi
This API is used to delete an entry from tbl_subscribers_groups in the Subscription Group Lookup page
=cut

post '/subscription_lookup/delete/:subscription_id/:imsi' => sub {
	
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'E') || has_access("/subscription_lookup"));
	
	my $subscription_id = params->{subscription_id};
	my $imsi = params->{imsi};
	
	my $query = "DELETE FROM tbl_subscribers_groups WHERE subscription_id =? AND imsi =?";

	my @sql_params = ($subscription_id,$imsi);

	my $res = Invigo::execute_sql($dbh,$query,@sql_params);

	
	return api_error("Could not delete entry!") if $res == 0;

	Invigo::set_db_update_time("TBL_SUBSCRIBERS_GROUPS");

	return {
		result => "OK"
	} if $res == 1;
	
};


post '/subscription_lookup/advanced_delete' => sub {

	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'E') || has_access("/subscription_lookup"));

	my $sql_check = "SELECT EXISTS (SELECT 1 FROM TBL_SUBSCRIBERS_GROUPS WHERE imsi = ?)";
	my $query = "DELETE FROM tbl_subscribers_groups where imsi = ?";

	if (defined params->{filename}){
		return api_error("Wrong File format") unless (params->{filename}=~ m/\.txt$/ || params->{filename}=~ m/\.csv$/);
		my $file_path=$config->{UPLOAD_FOLDER}."/".params->{file};
		open(my $uploadfile, "<", $file_path) or return api_error("FILE_NOT_FOUND");
		local $/;
		my @records;
		if(params->{filename}=~ m/\.csv$/){
		 	@records  = split("\n",<$uploadfile>);
		 	map { s/\s$//g } @records;
		}else{
			@records  = split(/[, \n]/,<$uploadfile>);
			map { s/\s$//g } @records;
		}
		if(scalar grep(!/^(\d+|(\d+)-(\d+))$/,@records) > 0){
			unlink($file_path);
			return api_error("file is not formatted correctly");
		}
		params->{imsi_input}=\@records;
		unlink($file_path);
	}
	my @imsis = @{params->{imsi_input}};

	return api_error("List of imsis is empty.") if scalar @imsis eq 0;

	my %unique_imsis;

	my $unsuccessful_imsis = [];
	my $sqls_to_execute = [];


	#Check if submitted values include ranges, process the imsis, remove duplicates
	foreach my $entry (@imsis){
		if ($entry =~ /(\d+)-(\d+)/) {
			# It's a range
			my ($start, $end) = ($1, $2);

			#Add IMSIs as keys
			$unique_imsis{$_} = 1 for $start .. $end;

			
    	}elsif ($entry =~ /(\d+)/) {
			#Add IMSIs as keys
			$unique_imsis{$1} = 1;
    	}
	}

	#Add IMSI keys to array
	my @imsis_to_add = keys %unique_imsis;

	#Process IMSIs array
	foreach my $imsi (@imsis_to_add){
		if(Invigo::get_rows_from_db($dbh,$sql_check,$imsi)->[0][0]){
			my @sql_params = ($imsi);
			push @$sqls_to_execute,[$query,($imsi)];
		}else{
				push @$unsuccessful_imsis,$imsi;
		}
	}
	
	return api_error("One or more IMSIs do not exist!") if scalar @$unsuccessful_imsis > 0;

	my $res = Invigo::execute_sqls($dbh,@$sqls_to_execute);

	return api_error("An error occured with one or more IMSIs.") if $res != scalar @$sqls_to_execute;

	Invigo::set_db_update_time("TBL_SUBSCRIBERS_GROUPS");

	return{
		result => "OK",
	} if $res >0;
};



post '/subscription_lookup/add' => sub {

	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'E') || has_access("/subscription_lookup"));

	my $sql_check = "SELECT EXISTS (SELECT 1 FROM TBL_SUBSCRIBERS_GROUPS WHERE imsi = ?)";
	my $query = "INSERT INTO tbl_subscribers_groups (subscription_id,imsi) VALUES (?,?)";

	my $subscription_id = params->{subscription};

	if (defined params->{filename}){
		return api_error("Wrong File format") unless (params->{filename}=~ m/\.txt$/ || params->{filename}=~ m/\.csv$/);
		my $file_path=$config->{UPLOAD_FOLDER}."/".params->{file};
		open(my $uploadfile, "<", $file_path) or return api_error("FILE_NOT_FOUND");
		local $/;
		my @records;
		if(params->{filename}=~ m/\.csv$/){
		 	@records  = split("\n",<$uploadfile>);
		 	map { s/\s$//g } @records;
		}else{
			@records  = split(/[, \n]/,<$uploadfile>);
			map { s/\s$//g } @records;
		}
		if(scalar grep(!/^(\d+|(\d+)-(\d+))$/,@records) > 0){
			unlink($file_path);
			return api_error("file is not formatted correctly");
		}
		params->{imsi_input}=\@records;
		unlink($file_path);
	}
	my @imsis = @{params->{imsi_input}};

	return api_error("List of imsis is empty.") if scalar @imsis eq 0;

	my %unique_imsis;

	my $unsuccessful_imsis = [];
	my $sqls_to_execute = [];


	#Check if submitted values include ranges, process the imsis, remove duplicates
	foreach my $entry (@imsis){
		if ($entry =~ /(\d+)-(\d+)/) {
			# It's a range
			my ($start, $end) = ($1, $2);

			#Add IMSIs as keys
			$unique_imsis{$_} = 1 for $start .. $end;

			
    	}elsif ($entry =~ /(\d+)/) {
			#Add IMSIs as keys
			$unique_imsis{$1} = 1;
    	}
	}

	#Add IMSI keys to array
	my @imsis_to_add = keys %unique_imsis;

	#Process IMSIs array
	foreach my $imsi (@imsis_to_add){
		if(!Invigo::get_rows_from_db($dbh,$sql_check,$imsi)->[0][0]){
			my @sql_params = ($subscription_id,$imsi);
			push @$sqls_to_execute,[$query,($subscription_id,$imsi)];
		}else{
				push @$unsuccessful_imsis,$imsi;
		}
	}
	
	return api_error("Could not add entries! One or more IMSIs already exist.") if scalar @$unsuccessful_imsis > 0;

	my $res = Invigo::execute_sqls($dbh,@$sqls_to_execute);

	return api_error("An error occured with one or more IMSIs.") if $res != scalar @$sqls_to_execute;

	Invigo::set_db_update_time("TBL_SUBSCRIBERS_GROUPS");

	return{
		result => "OK",
	} if $res >0;
};

post '/subscription_lookup/edit' => sub {

	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'E') || has_access("/subscription_lookup"));

	my $query = "UPDATE tbl_subscribers_groups SET subscription_id=? WHERE imsi=?";

	my $subscription_id = params->{subscription};
	my $imsi = params->{imsi};

	my $sql_imsi_check = "SELECT EXISTS (SELECT 1 FROM TBL_SUBSCRIBERS_GROUPS WHERE imsi = ?)";
	my $sql_subscription_check = "SELECT EXISTS (SELECT 1 FROM rt_subscription WHERE id = ?)";

	if(!Invigo::get_rows_from_db($dbh,$sql_imsi_check,$imsi)->[0][0]){
		return api_error("IMSI does not exist in database!");
	} elsif (!Invigo::get_rows_from_db($dbh,$sql_subscription_check,$subscription_id)->[0][0]){
		return api_error("Subscription does not exist in database!");
	} else {
		my $res = Invigo::execute_sqls($dbh,$query,($subscription_id,$imsi));

		Invigo::set_db_update_time("TBL_SUBSCRIBERS_GROUPS");

		return {
			result => "OK"
		}
	}

	return api_error("An error occured");
};

post '/subscription_lookup/download_imsis_per_group' => sub {

	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access("/subscription_lookup"));

	my $query =	"SELECT rt.id,rt.name,sg.imsi FROM rt_subscription rt JOIN tbl_subscribers_groups sg ON rt.id = sg.subscription_id WHERE sg.subscription_id=?";

	my $subscription_id = params->{subscription};

	my $db_imsis = Invigo::get_rows_from_db($query,$subscription_id);

	return api_error("Could not find imsis with this subscription") if scalar @$db_imsis == 0;
	return {
		result => "OK",
		imsis => $db_imsis,
	} if scalar @$db_imsis > 0;

};

############################################################################################################################################################

=head2 get /assign_messages/config
This API is used to retrieve the configs for the assign_messages 
=cut


get '/assign_messages/config' => sub {

	return api_error('ACTION_NOT_ALLOWED') unless has_access("ACCESS_API", 'V') || has_access("/assign_messages");


	my @roamer_types = ();
	my $message_texts = {};

	foreach (@roamer_types_requested_order){
		push @roamer_types,$_ if ($roaming_types->{lc $_} && lc $_ ne 'all');
	}

	my $result;
	my @event_types= sort keys %{$event_type_to_state_hash};
	my @message_categories=sort keys %{$message_categories_to_table_hash};
	my $roamer_types_valid_messages = map_messages_to_roamer_types(\@roamer_types,\@event_types);
	my $roamer_types_valid_message_categories = map_message_categories_to_roamer_types(\@roamer_types,\@message_categories);
	my $pane_columns = get_pane_columns(@roamer_types);
	my $fetch_groups_by_category=fetch_groups_by_category();
	my $groups=$cache->{GROUPS};

	foreach my $id (keys %{$cache->{tbl_all_messages}}){	
		$message_texts->{$id} = {};
		foreach my $lang (keys %{$cache->{tbl_all_messages}->{$id}}){
			$message_texts->{$id}->{$lang} = $cache->{tbl_all_messages}->{$id}->{$lang}->{TEXT};
		}
	}

	my $messages=Invigo::get_table_hash('tbl_all_messages' , {id=>'ID' , cols=>'INFO' });
	$result={
		config => {
			sms_list_max => $config->{sms_list_max},
			message_details_products => $config->{message_details_products} || 0,
			message_details_mvno => $config->{message_details_mvno} || 0,
			message_details_nationality => $config->{message_details_nationality} || 0,
		},
		messages=> $messages,
		message_texts => $message_texts,
		columns => $pane_columns,
		roamer_types => \@roamer_types,
		event_type_to_state => $event_type_to_state_hash,
		roamer_types_messages_map => $roamer_types_valid_messages,
		roamer_types_messages_categories_map => $roamer_types_valid_message_categories,
		cache_groups_by_criteria=> $fetch_groups_by_category,
		group=>$groups
	};

	$result->{countries} = $cache->{interface_cache}->{countries} if $countries_changed;
	$result->{operators} = $cache->{interface_cache}->{operators} if $operators_changed;

	$result->{roamer_profile}->{subscriptions} = $cache->{rt_subscription};
	if ($config->{message_details_products}){
		$result->{roamer_profile}->{product_categories} = $cache->{rt_product_category};
		$result->{roamer_profile}->{roaming_products} = $cache->{rt_roaming_product_id_to_name};
	}
	if($config->{message_details_mvno}){
		$result->{roamer_profile}->{mvno} = $dbh->selectall_arrayref("SELECT DISTINCT ID, NAME FROM TBL_MVNO_RANGES", {Slice => {}});
	}
	$result->{roamer_profile}->{nationalities} = $cache->{rt_nationality} if $config->{message_details_nationality} ;
	return $result;
};

=head2 get /assign_messages/:roamer_type/rows
This API is used to retrieve the data for the assign_messages 
=cut
get '/assign_messages/:roamer_type/rows' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access('ACCESS_API','V') || has_access("/assign_messages"));
	
	my @params = ();
	my ($rows, $query);
	my $messages=[];

	my @message_categories=sort keys %{$message_categories_to_table_hash};
	my $roamer_types_valid_message_categories = map_message_categories_to_roamer_types([params->{roamer_type}],\@message_categories)->{params->{roamer_type}};
	return api_error("Roamer type invalid") if scalar (@$roamer_types_valid_message_categories) eq 0;
	my @roamer_profile_keys=@{$config->{message_details_extra_columns}};
	
	foreach my $category (@$roamer_types_valid_message_categories){
		$query="select CONCAT_WS(',',in_out,visiting_back),message_id".(($message_categories_to_table_hash->{$category}->{key}) ne '' ?",array_agg(".$message_categories_to_table_hash->{$category}->{key}.")":"")." from ".$message_categories_to_table_hash->{$category}->{tbl}." where in_out='".$roaming_types->{params->{roamer_type}}."'AND  (MESSAGE_ID <> 0 AND MESSAGE_ID <> 99) GROUP BY MESSAGE_ID,IN_OUT,VISITING_BACK";
		$rows= get_rows_from_db($dbh,$query);
		foreach my $row (@$rows) {
			my $hash={message_category=>$category,message_type=>$state_to_event_hash->{$row->[0]},message_id=>$row->[1]};
			$hash->{$message_categories_to_table_hash->{$category}->{key}}=$row->[2] unless $message_categories_to_table_hash->{$category}->{key} eq '';
			push @$messages,$hash;
		}
	}

	foreach my $message (@$messages){
		$query = "select m.user_comment,d.type,".join(',',@roamer_profile_keys).",array_agg(d.text) FROM tbl_messages m LEFT JOIN tbl_message_details d ON m.message_id = d.id where m.message_id=".$message->{message_id}." GROUP BY m.message_id,d.type,".join(',',@roamer_profile_keys).";";
		$rows=get_rows_from_db($dbh,$query);
		$message->{roamer_profiles}=[];
		foreach my $row (@$rows){
			$message->{title}=$row->[0] if !defined($message->{title});
			$message->{type}=$row->[1] if !defined($message->{type});
			my $roamer_profile={};
			foreach (0 .. $#roamer_profile_keys){
				$roamer_profile->{lc $roamer_profile_keys[$_]}=$row->[$_+2];
			}
			$roamer_profile->{sms_list}=$row->[scalar @$row-1];
			push @{$message->{roamer_profiles}},$roamer_profile;
		}
	}
	#TODO return this as an array of hashes

	return {rows=>$messages};

};

=head2 [post,put] /assign_messages/add
This API is used to add messages for the assign_messages 
=cut

any ['post', 'put'] => '/assign_messages/add' => sub {

	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'E') || has_access('/smswelcome/assign_messages/','E'));

	my $sqls_to_execute = [];
	my $sqls_to_check_duplicate = [];

	my ($query,$check_query,@values,$message_id,$result,$type,$state,$old_type,$old_state,$category,$old_category,$action);

	$category = $message_categories_to_table_hash->{params->{message_category}} if defined params->{message_category} && defined $message_categories_to_table_hash->{params->{message_category}};
	$old_category = $message_categories_to_table_hash->{params->{old}->{message_category}} if defined(params->{old}) && defined params->{old}->{message_category} && defined $message_categories_to_table_hash->{params->{old}->{message_category}};
	($type,$state) = split ',', $event_type_to_state_hash->{params->{message_type}} if defined params->{message_type} && defined $event_type_to_state_hash->{params->{message_type}};
	($old_type,$old_state) = split ',', $event_type_to_state_hash->{params->{old}->{message_type}} if defined(params->{old}) && defined params->{old}->{message_type} && defined $event_type_to_state_hash->{params->{old}->{message_type}};

	my @roamer_profile_keys=@{$config->{message_details_extra_columns}};
	my $array_compare = Array::Compare->new;

	if (defined params->{filename}){
		return api_error("Wrong File format") unless (params->{filename}=~ m/\.txt$/ || params->{filename}=~ m/\.csv$/);
		my $file_path=$config->{UPLOAD_FOLDER}."/".params->{file};
		open(my $uploadfile, "<", $file_path) or return api_error("FILE_NOT_FOUND");
		local $/;
		my @records;
		if(params->{filename}=~ m/\.csv$/){
		 	@records  = split("\n",<$uploadfile>);
		 	map { s/\s$//g } @records;
		}else{
			@records  = split(/[, \n]/,<$uploadfile>);
			map { s/\s$//g } @records;
		}
		if(scalar grep(!/^\d+$/,@records) > 0){
			unlink($file_path);
			return api_error("file is not formatted correctly");
		}
		params->{$category->{key}}=\@records;
		unlink($file_path);
	}
	if (defined params->{custom_group_id}){
		my @records=fetch_bulk_from_group(params->{message_category},params->{custom_group_id});
		if(!(scalar @records > 0)){
			return api_error("Group ID not associated with Category ".params->{message_category}." or Group ID not found");
		}
		params->{$category->{key}}=\@records;
	}

	my @required_parameters = ("title","message_type","message_category");
	push @required_parameters, $category->{key} unless !$category->{key} && defined ($category->{key});
	
	foreach(@required_parameters){ 
		return api_error("INVALID_OR_MISSING_FIELD") unless defined(params->{$_}) && params->{$_} ne "";
	}
	
	return api_error("Message category invalid") if !defined($category);
	return api_error("Roamer type invalid") if !defined($type);
	return api_error("Roamer state invalid") if !defined($state);

	params->{$category->{key}} = [params->{$category->{key}}] if ref(params->{$category->{key}}) ne 'ARRAY' && 	($category->{key} && defined ($category->{key}));
	if(!defined(params->{message_id})){
		return api_error("INVALID_OR_MISSING_FIELD") unless defined(params->{$category->{key}}) || $category->{key} eq '';
		return api_error("INVALID_OR_MISSING_FIELD") unless $category->{key} eq '' || scalar(@{params->{$category->{key}}}) gt 0;

		$message_id = get_value_from_db(get_sql('get_nextval', 'SEQ_MESSAGE_ID'));
		$message_id = get_value_from_db(get_sql('get_nextval', 'SEQ_MESSAGE_ID')) if $message_id eq '99';
		push @$sqls_to_execute,["INSERT INTO TBL_MESSAGES VALUES (?,?)",$message_id,params->{title}];

		$action="insert";
	}
	elsif (defined(params->{message_id})){
		$message_id=params->{message_id};
		push @$sqls_to_execute,["UPDATE TBL_MESSAGES SET user_comment=? where message_id=?",params->{title},$message_id];
		push @$sqls_to_execute , ["DELETE FROM TBL_MESSAGE_DETAILS WHERE ID = ?", params->{message_id}];
		
		return api_error("INVALID_OR_MISSING_FIELD") unless (($category->{key} eq '' ||  (defined(params->{$category->{key}}) && scalar(@{params->{$category->{key}}}) gt 0)) && ( $old_category->{key} eq '' || (defined(params->{old}->{$old_category->{key}}) && scalar(@{params->{old}->{$old_category->{key}}}) gt 0))) || (params->{old}->{message_category} ne params->{message_category});
		if((defined(params->{old}->{message_category}) && (params->{old}->{message_category} ne params->{message_category})) || ( defined(params->{old}->{$category->{key}}) && !$array_compare->simple_compare(params->{$category->{key}}, params->{old}->{$category->{key}})) ){
			push @$sqls_to_execute,["DELETE FROM ".((defined(params->{old}->{message_category}) && (params->{old}->{message_category} ne params->{message_category}))? $message_categories_to_table_hash->{params->{old}->{message_category}}->{tbl} : $category->{tbl})." WHERE MESSAGE_ID = ? AND IN_OUT=? AND VISITING_BACK = ?", params->{message_id}, $old_type ,$old_state];
			$action='insert';
		}
		else {
			$action="update";
		}
	}

	foreach (@{params->{roamer_profiles}}){
		my $roamer_profile=$_;
		return api_error("INVALID_OR_MISSING_FIELD") unless scalar @{$roamer_profile->{sms_list}};
		$query="INSERT INTO TBL_MESSAGE_DETAILS (id,type,".join(',',@roamer_profile_keys).",sendorder,text) VALUES (?,?,".join(',',('?') x scalar @roamer_profile_keys).",?,?)";
		@values=($message_id,$cache->{message_type}->{SMS});
		foreach (@roamer_profile_keys){
			if(defined($roamer_profile->{lc $_})){
				push @values, $roamer_profile->{lc $_};
			}
			else{
				push @values, 0;
			}
		}
		foreach (0 .. $#{$roamer_profile->{sms_list}}){
			return api_error("Empty Message Exists") unless scalar keys %{$cache->{tbl_all_messages}->{$roamer_profile->{sms_list}[$_]}};
			push @values, $_;
			push @values, $roamer_profile->{sms_list}[$_];
			push @$sqls_to_execute,[$query,@values];
			pop @values; 
			pop @values;
		}
	}

	$check_query = "SELECT count(*) FROM ".$category->{tbl}." where (IN_OUT = ? or IN_OUT='A') AND (VISITING_BACK = ? or VISITING_BACK='A') AND MESSAGE_ID=0 ".(($category->{key}) ne '' ?" AND ".$category->{key}." IN (".join(',',('?') x scalar @{params->{$category->{key}}}).")":"");
	$sqls_to_check_duplicate = [$check_query,$type,$state] unless defined(params->{$category->{key}}) && scalar(@{params->{$category->{key}}}) gt 0;
	$sqls_to_check_duplicate = [$check_query,$type,$state,@{params->{$category->{key}}}] if defined($category->{key}) && defined(params->{$category->{key}}) && ref params->{$category->{key}} eq 'ARRAY' && scalar(@{params->{$category->{key}}}) gt 0;
	my $res = Invigo::get_rows_from_db($dbh,@$sqls_to_check_duplicate);
	return api_error("Record Blacklisted") if $res && $res->[0]->[0];

	$check_query = "SELECT MESSAGE_ID FROM ".$category->{tbl}." where IN_OUT = ? AND VISITING_BACK = ? AND MESSAGE_ID!=0 ".(($category->{key}) ne '' ?" AND ".$category->{key}." IN (".join(',',('?') x scalar @{params->{$category->{key}}}).")":"");
	$sqls_to_check_duplicate = [$check_query,$type,$state] unless defined(params->{$category->{key}}) && scalar(@{params->{$category->{key}}}) gt 0;
	$sqls_to_check_duplicate = [$check_query,$type,$state,@{params->{$category->{key}}}] if defined($category->{key}) && defined(params->{$category->{key}}) && ref params->{$category->{key}} eq 'ARRAY' && scalar(@{params->{$category->{key}}}) gt 0;
	
	if($action eq 'insert'){
		$query="INSERT INTO ".$category->{tbl}." (IN_OUT,VISITING_BACK,MESSAGE_ID".(($category->{key}) ne '' ?",".$category->{key}:"").") VALUES (?,?,?";
		$query .= (($category->{key}) ne '' ? ",?)":")");
		if($category->{key} ne ''){
			push @$sqls_to_execute,[$query,$type,$state,$message_id,$_] foreach (@{params->{$category->{key}}});
		}else{
			push @$sqls_to_execute,[$query,$type,$state,$message_id];
		}
	}
	elsif( $action eq 'update'){
		$query="UPDATE ".$category->{tbl}." SET IN_OUT=? , VISITING_BACK=? WHERE in_out=? and visiting_back=? and message_id=?";
		push @$sqls_to_execute,[$query,$type,$state,$old_type,$old_state,$message_id];
	}

	$res=Invigo::execute_sqls($dbh,@$sqls_to_execute);
	$result={status => "OK",message_id=>$message_id};
	$result->{$category->{key}}=params->{$category->{key}} if (defined params->{custom_group_id} || defined params->{filename}) && defined params->{$category->{key}};
	if ($res){
		Invigo::set_db_update_time("BLACKLIST_".$category->{tbl});
		Invigo::set_db_update_time("TBL_MESSAGE_DETAILS");
		return $result;
	}

	$res = Invigo::get_rows_from_db($dbh,@$sqls_to_check_duplicate);
	return api_error("Cannot Have Duplicate Records!")if $res && $res->[0]->[0];
	return api_error("Message Add/Update failed");
};

=head2 get /assign_messages/delete/
This API is used to delete messages for the assign_messages 
=cut

post '/assign_messages/delete/' => sub {
  
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'D') || has_access('/smswelcome/assign_messages/','D'));
	return api_error("INVALID_ID") unless defined(params->{message_id});

	my $sqls_to_execute = [];
	my ($category,$type,$state);
	$category=$message_categories_to_table_hash->{params->{message_category}};
	return api_error("INVALID_OR_MISSING_FIELD") if !defined($category);

	($type,$state)= split ',', $event_type_to_state_hash->{params->{message_type}} if defined(params->{message_type}) && defined($event_type_to_state_hash->{params->{message_type}});
	return api_error("INVALID_OR_MISSING_FIELD") if !defined($type) || !defined($state) || !defined($event_type_to_state_hash->{params->{message_type}});

	push @$sqls_to_execute , ["DELETE FROM ".$category->{tbl}." WHERE MESSAGE_ID = ? AND IN_OUT=? AND VISITING_BACK = ?", params->{message_id}, $type, $state];
	push @$sqls_to_execute , ["DELETE FROM TBL_MESSAGE_DETAILS WHERE ID = ?", params->{message_id}];
	push @$sqls_to_execute , ["DELETE FROM TBL_MESSAGES WHERE MESSAGE_ID = ?", params->{message_id}];

	my $res = Invigo::execute_sqls($dbh,@$sqls_to_execute);
	if ($res){
		Invigo::set_db_update_time("BLACKLIST_".$category->{tbl});
		Invigo::set_db_update_time("TBL_MESSAGE_DETAILS");
		return {status => "OK"};
	}
	return api_error("Message Delete failed");

};

=head2 get /sending_control/config
This API is used to retrieve the required configs for the Message Frequency Control Page
=cut
get '/sending_control/config' => sub {
	
	return api_error('ACTION_NOT_ALLOWED') unless has_access('ACCESS_API','V') || has_access('smswelcome/sending_control/','V');

	my ($result,@operators,@countries,@roamer_types,$cell_ids,$limit_categories);
	
	my @event_types= sort keys %{$event_type_to_state_hash};
	my @limit_categories=keys %{$limit_categories_to_table_hash};
	
	$cell_ids = get_rows_from_db($dbh, "SELECT * FROM RT_CELL_GROUPS ORDER BY ID", {arrayref_of_hashrefs => 1}) if ($config->{manage_cell_id_in} || $config->{manage_cell_id_out});

	foreach (@roamer_types_requested_order){
		push @roamer_types,$_ if $roaming_types->{lc $_};
	}

	@$cell_ids = map {{ id => $_->{ID}, name => $_->{NAME} }} @$cell_ids;

	my $roamer_types_valid_messages = map_messages_to_roamer_types(\@roamer_types,\@event_types);
	my $reminder_frequency_active = { 
		inbound => $config->{send_inbound_refresh_message} || 0, 
		outbound => $config->{send_outbound_refresh_message} || 0,
		all=>0
	};

	# TODO: Implement Panes columns
	$result = {
		cell_ids => $cell_ids || [],
		event_type_to_state_hash => $event_type_to_state_hash,
		roamer_types_message_map => $roamer_types_valid_messages,
		limit_categories=> $limit_categories_to_table_hash,
		subscriptions => $cache->{rt_subscription},
		frequency_message_active => $config->{limit_sms_per_location} || 0,
		reminder_frequency_active => $reminder_frequency_active,
	};

	$result->{countries} = $cache->{interface_cache}->{countries} if $countries_changed;
	$result->{operators} = $cache->{interface_cache}->{operators} if $operators_changed;

	return $result; 

};

=head2 get /sending_control/:pane/rows
This API is used to fetch rows of frequency limits or reminder message
=cut
get '/sending_control/:pane/rows' => sub {
	
	return api_error('ACTION_NOT_ALLOWED') unless has_access('ACCESS_API','V') || has_access('smswelcome/sending_control/','V');
	my $pane = params->{pane};
	return api_error('BAD_PARAMETERS') unless $pane =~ /^(message_limits|reminder_message)$/;

	my $rows;
	if($pane eq 'message_limits'){
		$rows = get_rows_from_db($dbh, "SELECT CONCAT_WS(',',in_out, visiting_back, msisdn, subscription, mcc, hni, cell_id_group),CONCAT_WS(',',in_out, visiting_back),msisdn,subscription, mcc, hni, cell_id_group,sms_limit,resend_period FROM TBL_MESSAGE_LIMITS");
	}elsif($pane eq 'reminder_message'){
		#Visiting Back is no longer in the ID, but is still in the DB (not part of the primary key)
		$rows = get_rows_from_db($dbh, "SELECT CONCAT_WS(',',in_out, msisdn, subscription, mcc, hni, cell_id_group),CONCAT_WS(',',in_out, visiting_back),msisdn, subscription, mcc, hni, cell_id_group,refresh_period FROM TBL_MESSAGE_REFRESH_PERIODS");
	}

	foreach (@$rows){
		$_->[1]=$state_to_event_hash->{$_->[1]};
	}

	return api_error("No records Found!") if !defined($rows);
	return {result => "OK", records => $rows};

};

=head2 post /sending_control/:pane/add
This API is used to add or edit new records for frequency limits or refresh message
=cut
post '/sending_control/:pane/add' => sub {

	my $pane = params->{pane};
	my @required_parameters;
	my ($sql,$result,$category_specific,$old_category_specific,$old_type,$old_state,$type,$state);

	return api_error('ACTION_NOT_ALLOWED') unless has_access('ACCESS_API','E') || has_access('smswelcome/sending_control/','E');
	return api_error('BAD_PARAMETERS') unless $pane =~ /^(message_limits|reminder_message)$/ ;

	$category_specific = $limit_categories_to_table_hash->{params->{message_category}};
	return api_error('BAD_PARAMETERS') unless defined $category_specific && defined params->{lc $category_specific} || lc params->{message_category} eq "default";

	if (defined(params->{old})){
		($old_type,$old_state)= split ',', $event_type_to_state_hash->{params->{old}->{message_type}};
		$old_category_specific = $limit_categories_to_table_hash->{params->{old}->{message_category}};
	}

	($type,$state)= split ',', $event_type_to_state_hash->{params->{message_type}} if defined(params->{message_type}) && defined($event_type_to_state_hash->{params->{message_type}});

	$result = { result => "OK" };

	if($pane eq 'message_limits'){
	
		@required_parameters = ("message_type","resend_period","sms_limit");
		foreach (@required_parameters) {
			return api_error("INVALID_OR_MISSING_FIELD") unless defined params->{$_};
			return api_error("INVALID_OR_MISSING_FIELD") unless !defined params->{old} || defined params->{old}->{$_};
		}

		my @sql_params = ($type,$state,params->{sms_limit},params->{resend_period});
		push @sql_params,params->{lc $category_specific} if $category_specific ne '';

		if (defined params->{old}){
			$sql = "update tbl_message_limits set in_out = ?, visiting_back = ?, sms_limit = ?, resend_period = ? ";
			$sql .= ", $category_specific = ? " if $category_specific ne '';
			$sql .= ", $old_category_specific = 0 " if $old_category_specific ne $category_specific && $old_category_specific ne '';
			$sql .= "WHERE in_out = ? AND visiting_back = ?" ;
			$sql .= $old_category_specific ne '' ? " AND $old_category_specific = ?":"" ;
			push @sql_params,($old_type,$old_state);
			push @sql_params, params->{old}->{lc $old_category_specific} if $old_category_specific ne '';
		}else{
			$sql = "insert into tbl_message_limits(in_out,visiting_back,sms_limit,resend_period";
			$sql .=$category_specific ne '' ? ",$category_specific) VALUES (?,?,?,?,?)":") VALUES (?,?,?,?)";
		}
		my $res = Invigo::execute_sql($dbh,$sql,@sql_params);
		
		return $result if $res;
		return api_error("Couldn't add a new frequency control") unless defined params->{old};
		return api_error("Couldn't update the new record");
	
	}elsif ($pane eq 'reminder_message'){

		@required_parameters = ("message_type","refresh_period");
		foreach (@required_parameters) {
			return api_error("INVALID_OR_MISSING_FIELD") unless defined params->{$_};
			return api_error("INVALID_OR_MISSING_FIELD") unless !defined params->{old} || defined params->{old}->{$_};
		}

		my @sql_params = ($type,$state,params->{refresh_period});
		push @sql_params,params->{lc $category_specific} if $category_specific ne '';

		if (defined params->{old}){
			$sql = "update tbl_message_refresh_periods set in_out = ?,visiting_back=?,refresh_period = ? ";
			$sql .= ", $category_specific = ? " if $category_specific ne '';
			$sql .= ", $old_category_specific = 0 " if $old_category_specific ne $category_specific && $old_category_specific ne '';
			$sql .= "WHERE in_out = ? AND visiting_back=? ";
			$sql .= $old_category_specific ne '' ?"AND $old_category_specific = ?":"";
			push @sql_params,($old_type,$old_state);
			push @sql_params, params->{old}->{lc $old_category_specific} if $old_category_specific ne ''
		}
		else{
			$sql = "insert into tbl_message_refresh_periods(in_out,visiting_back,refresh_period";
			$sql .=$category_specific ne '' ? ",$category_specific) VALUES (?,?,?,?)":") VALUES (?,?,?)";
		}

		my $res = Invigo::execute_sql($dbh,$sql,@sql_params);

		if ($res) {
			Invigo::set_db_update_time("TBL_MESSAGE_REFRESH_PERIODS") if $pane eq 'reminder_message';
			return $result;
		}
		return api_error("Couldn't add a new reminder rule") unless defined params->{old};
		return api_error("Couldn't update the reminder rule");
	}

};

=head2 post /sending_control/:pane/delete
This API is used to delete records for frequency limits or refresh message
=cut
post '/sending_control/:pane/delete' => sub {
	
	my $pane = params->{pane};
	my ($sql,$res,$result,@sql_params);
	
	$result = {result => "OK"};

	return api_error('ACTION_NOT_ALLOWED') unless has_access('ACCESS_API','D') || has_access('smswelcome/sending_control/','D');
	return api_error('BAD_PARAMETERS') unless $pane =~ /^(message_limits|reminder_message)$/;
	return api_error('INVALID_OR_MISSING_FIELD') unless defined params->{id};
	
	@sql_params = split ',',params->{id};
	if($pane eq 'message_limits'){
		$sql = "DELETE FROM tbl_message_limits WHERE in_out = ? AND visiting_back = ? AND msisdn = ? AND subscription = ? AND mcc = ? AND hni = ? AND cell_id_group = ?";
		$res = Invigo::execute_sql($dbh,$sql,@sql_params);

		return $result if $res;
		return api_error("Couldn't delete the frequency control rule");

	}elsif ($pane eq 'reminder_message'){
		#Visiting Back is no longer in the ID when receiving for delete, but is still in the DB (not part of the primary key)
		$sql = "DELETE FROM tbl_message_refresh_periods WHERE in_out = ? AND msisdn = ? AND subscription = ? AND mcc = ? AND hni = ? AND cell_id_group = ?";
		$res = Invigo::execute_sql($dbh,$sql,@sql_params);

		if ($res){
			Invigo::set_db_update_time("TBL_MESSAGE_REFRESH_PERIODS") ;
			return $result ;
		}
		return api_error("Couldn't delete the reminder message rule");
	}
};

#####################################################################################
############################ ---- Zones ---- ############################
#####################################################################################

=head2 get /zones/fetch_mcc_hni
This API is used to retrieve $cache->{mcc_hni_operators} and send it to the frontend
=cut

get '/zones/fetch_mcc_hni' => sub {
	my $result;

	$result->{mcc_hni} = $cache->{mcc_hni_operators} if $cache->{mcc_hni_operators};
	if ($result->{mcc_hni}){
		$result->{result} = "OK";
		return $result;
	}
	
	return api_error("Couldn't retrieve the MCC HNI operators");


};

#####################################################################################
############################ ---- Special Messagess ---- ############################
#####################################################################################


=head2 get /special_messages/config
This API is used to retrieve the required configs for Special Messages like Embassy and Tariff Messages
=cut
get '/special_messages/config' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless has_access('ACCESS_API','V') || has_access('smswelcome/special_messages/','V');
	
	my (@roamer_types,$result);
	foreach (@roamer_types_requested_order){
		push @roamer_types,$_ if ($roaming_types->{lc $_} && lc $_ ne 'all');
	}

	my @event_types= sort keys %{$event_type_to_state_hash};
	my @message_categories=sort keys %{$message_categories_to_table_hash};
	my $roamer_types_valid_messages = map_messages_to_roamer_types(\@roamer_types,\@event_types);
	my $roamer_types_valid_message_categories = map_message_categories_to_roamer_types(\@roamer_types,\@message_categories);
	
	my $panes = { embassy => {rows => [], order => 2,info => {columns => [{ ID => "ID",HIDE=>1},{ ID => "Country", WIDTH => 150},{ ID => "Operator", WIDTH => 220},{ ID => "Language", WIDTH => 120},{ ID => "Embassy Message", WIDTH => 350}]}}};
	$panes->{tariff} = {rows => [], order => 3,info => {columns => [{ ID => "ID",HIDE=>1},{ ID => "Country", WIDTH => 150},{ ID => "Operator", WIDTH => 220},{ ID => "Subscription", WIDTH => 220},{ ID => "Language", WIDTH => 120}, { ID => "Message", WIDTH => 350}]}}  if $config->{append_to_extra_vars}->{tariff} && !$config->{country_zones_enabled};
	$panes->{tariff} = {rows => [], order => 3,info => {columns => [{ ID => "ID",HIDE=>1},{ ID => "Zone", WIDTH => 220},{ ID => "Subscription", WIDTH => 220},{ ID => "Language", WIDTH => 120}, { ID => "Message", WIDTH => 350}]}}  if $config->{append_to_extra_vars}->{tariff} && $config->{country_zones_enabled};
	$panes->{region} = {rows => [], order => 1,info => {columns => [{ ID => "ID",HIDE=>1},{ ID => "Message Type", WIDTH => 130},{ ID => "Region ID", WIDTH => 220},{ ID => "Language", WIDTH => 120}, { ID => "Message", WIDTH => 350}]}};

	$result = {
		result => "OK",
		languages => $cache->{rt_language},
		subscriptions => $cache->{rt_subscription},
		regions => $cache->{tbl_region},
		region_valid_messages=>$roamer_types_valid_messages->{inbound},
		message_event_types_map => $roamer_types_valid_messages,
		panes => $panes
	};

	$result->{zones}= $cache->{rt_zones} if $config->{append_to_extra_vars}->{tariff} && $config->{country_zones_enabled};
	
	$result->{countries} = $cache->{interface_cache}->{countries} if $countries_changed;
	$result->{operators} = $cache->{interface_cache}->{operators} if $operators_changed;

	return $result;
};

=head2 post /special_messages/:special_msg_type/add
This API is used to add or update entries relating to special messages like Embassy, Region and Tariff Messages
=cut

post '/special_messages/:special_msg_type/add' => sub {

	#variable name for special message type

	my $special_msg_type = params->{special_msg_type};
	return api_error('ACTION_NOT_ALLOWED') unless has_access('ACCESS_API','A') || has_access('smswelcome/special_messages/','A');
	return api_error('BAD_PARAMETERS') unless $special_msg_type =~ /^(embassy|tariff|region)$/;

	my @sql_params = ();
	my $sqls_to_execute = [];
	my ($res,$sql,$new_id,$old_id) = ("","","",params->{id});

	if (defined(params->{message_type})){
		my ($type,$state) = split ',', $event_type_to_state_hash->{params->{message_type}};
		params->{in_out} = $type unless $special_msg_type eq 'region';
		params->{visiting_back} = $state;
	}
	if (defined(params->{id}) && $special_message_hash_table->{$special_msg_type}->{message_type_required}) {
		my @id= split(",",params->{id});
		$old_id= (split(',', $event_type_to_state_hash->{$id[0]}))[1].",".join(',',@id[1..$#id]);
	}
	foreach (@{$special_message_hash_table->{$special_msg_type}->{criteria}}){
		return api_error('MISSING_PARAMETERS') unless defined(params->{lc $_});
	}
	return api_error('MISSING_PARAMETERS') unless !defined(params->{id}) || (scalar(split(",",$old_id))) eq scalar @{$special_message_hash_table->{$special_msg_type}->{criteria}};
	$res = add_specific_tariff_message($dbh,params) if defined &add_specific_tariff_message;
	if (params->{special_msg_type} ne 'tariff' || !defined &add_specific_tariff_message){
		$new_id = join(",",map {params->{$_}} @{$special_message_hash_table->{$special_msg_type}->{criteria}});
		if(defined(params->{id})){
			push @sql_params, split(",",$new_id) if $old_id ne $new_id;
			push @sql_params, params->{message_text};
			push @sql_params, split(",",$old_id);
			my @sql_attributes = map { " $_ = ? " }  @{$special_message_hash_table->{$special_msg_type}->{criteria}};
			$sql = "UPDATE $special_message_hash_table->{$special_msg_type}->{table} set $special_message_hash_table->{$special_msg_type}->{message_attribute} = ? WHERE ".join("AND", @sql_attributes) unless $old_id ne $new_id;
			$sql = "UPDATE $special_message_hash_table->{$special_msg_type}->{table} set ".join(",", @sql_attributes).", $special_message_hash_table->{$special_msg_type}->{message_attribute} = ? WHERE".join("AND", @sql_attributes) if $old_id ne $new_id;
		}else{ 
			my $insert_columns=[@{$special_message_hash_table->{$special_msg_type}->{criteria}}];
			foreach(@$insert_columns){
				push @sql_params, params->{$_};
			}
			push @$insert_columns,$special_message_hash_table->{$special_msg_type}->{message_attribute};
			push @sql_params,params->{message_text};
			$sql = "INSERT INTO $special_message_hash_table->{$special_msg_type}->{table} (".join(",",@$insert_columns).") VALUES (".join(',',('?') x scalar @$insert_columns).")";
		}
		$res=Invigo::execute_sqls($dbh,$sql,@sql_params);
	}

	return api_error("Could not update record") if !$res && defined(params->{id});
	return api_error("Could not Add new record") if !$res && !defined(params->{id});
	Invigo::set_db_update_time($special_message_hash_table->{$special_msg_type}->{table});
	return { result => "OK" };

};

=head2 post /special_messages/:special_msg_type/delete
This API is used to delete entries relating to special messages like Embassy, Region and Tariff Messages
=cut

post '/special_messages/:special_msg_type/delete' => sub {
	
	my $res;
	my $special_msg_type = params->{special_msg_type};
	return api_error('ACTION_NOT_ALLOWED') unless has_access('ACCESS_API','A') || has_access('smswelcome/special_messages/','A');
	return api_error('MISSING_PARAMETERS') unless defined(params->{id}) && params->{id};
	return api_error('BAD_PARAMETERS') unless $special_msg_type =~ /^(embassy|tariff|region)$/;

	$res = delete_specific_tariff_message($dbh,params) if defined &delete_specific_tariff_message;

	if(params->{special_msg_type} ne 'tariff' || !defined &delete_specific_tariff_message){
		my @id = split(",",params->{id});
		params->{id}=(split(',', $event_type_to_state_hash->{$id[0]}))[1].",".join(',',@id[1..$#id]) if ($special_message_hash_table->{$special_msg_type}->{message_type_required} && $special_msg_type eq 'region');
		my @sql_params = split(",",params->{id});
		my @sql_attributes = map { " $_ = ? " }  @{$special_message_hash_table->{$special_msg_type}->{criteria}};
		$res = Invigo::execute_sql($dbh,"DELETE FROM $special_message_hash_table->{$special_msg_type}->{table} WHERE".join("AND", @sql_attributes),@sql_params);
	}
	
	return api_error("Could not delete record") if !$res;
	Invigo::set_db_update_time($special_message_hash_table->{$special_msg_type}->{table});
	return { result => "OK" };
};

=head2 post /special_messages/:special_msg_type/rows
This API is used to retrieve rows from the respective special message type tables:
TBL_EMBASSIES, TBL_Region_MESSAGES and TBL_TARIFF_MESSAGES
Returns each row as an array of arrays
=cut
get '/special_messages/:special_msg_type/rows' => sub {
	my $rows;
	my $special_msg_type = params->{special_msg_type};
	return api_error('ACTION_NOT_ALLOWED') unless has_access('ACCESS_API','A') || has_access('smswelcome/special_messages/','A');
	return api_error('BAD_PARAMETERS') unless params->{special_msg_type} =~ /^(embassy|tariff|region)$/;

	$rows = get_specific_tariff_message($dbh,params) if defined &get_specific_tariff_message;

	if (params->{special_msg_type} ne 'tariff' || !defined &get_specific_tariff_message){
		my $columns_to_fetch=[@{$special_message_hash_table->{$special_msg_type}->{criteria}},$special_message_hash_table->{$special_msg_type}->{message_attribute}];
		$rows = get_rows_from_db($dbh,"SELECT ".join(",",@{$columns_to_fetch})." FROM $special_message_hash_table->{$special_msg_type}->{table}");
	}

	if ($special_message_hash_table->{$special_msg_type}->{message_type_required} && $special_msg_type eq 'region'){
		foreach my $row (@$rows){
			my @temp_row=@$row;
			@temp_row = split(",", $state_to_event_hash->{"I,".$temp_row[0]}.",".join(',',@temp_row[1..$#temp_row]));
			$row = \@temp_row;
		}
	}

	return {result => "OK", rows => $rows};
};


########################################################################
####################### ---- Custom Reports ---- #######################
########################################################################


get '/custom_reports/:report_type/:report_name/:input' => sub {

	return api_error('ACTION_NOT_ALLOWED') unless has_access('ACCESS_API','A') || has_access('smswelcome/custom_reports/','A');
	return api_error("BAD_PARAMETERS") unless defined params->{report_type} && defined params->{report_name} && defined params->{input};

	my ($input, $query, $report_type, $report_name, $params, $path, $file, $datetime);

	$input = params->{input};
	$report_type = params->{report_type};
	
	if($report_type eq 'daily_stats'){

		my ($fields,@result,$stats);

		$query = {};
		$report_name = params->{report_name};
		$fields  = params->{fields};

		if ($input eq 'latest'){
			$query = { match_all => {}};
		}else{
			$query = { bool =>{ must => { match => { date => $input }}} };
		}
		
		@result = Invigo::Elastic::dump_index(Invigo::elastic_index('daily_stats'),$report_name.",date",$query,undef,[{ date => {order => 'desc'}}],{size => 1});
		return api_error("REPORT_NOT_FOUND") unless scalar @result;
		
		$stats = $result[0];

		upload_report_to_file($report_name,$fields,$stats->{$report_name},$config->{stats_folder},$stats->{date}, {count_report => 1});

	}

	$input =~ s/-//g;

	my $public_prod_downloads = "$FindBin::Bin/../public_production/downloads";
	my $public_dev_downloads = "$FindBin::Bin/../public_dev/downloads";
	$report_name = params->{report_name}.".csv";

	my ($from,$to) = split(",",$input);	
	if (!$to || $from eq $to) {
		$input = $from;
		$path = $config->{stats_folder}."/".$input;
		$file = path($path, $report_name);
		copy($file, "$public_prod_downloads/") or return api_error("Copy failed: $!");
	} else {
		my $unix_from = Invigo::date_to_unix($from);
		my $i = 0;
		my $tozip = "";
		my @sym_links;
		while (1) {
			my $day = Invigo::get_date_str({time=>$unix_from, date_separator=>"", days_offset=>$i});
			$path = $config->{stats_folder}."/".$day;
			$file = path($path, $report_name);
			my $sym_link = "$public_prod_downloads/".params->{report_name}."_$day.csv";
			if (-e $file) {
				system("ln -s $file $sym_link");
				$tozip .= " $sym_link";
				push @sym_links, $sym_link; 
			}
			$i++;
			last if ($day ge $to);
		}
		if ($tozip) {
			$report_name = params->{report_name} . "_". $from ."_". $to .".zip";
			unlink ("$public_prod_downloads/$report_name") if (-e "$public_prod_downloads/$report_name");
			my $r = system("zip -j $public_prod_downloads/$report_name $tozip");
			foreach (@sym_links) { unlink($_); }
			return api_error('ERROR_ZIPPING' , {status => 500 }) if ($r);
		} else {
			return api_error('NO_FILES_FOUND');
		}
	}

	if (-e $public_dev_downloads && -e "$public_prod_downloads/$report_name" ) {
		unlink ("$public_dev_downloads/$report_name") if (-e "$public_dev_downloads/$report_name");
		system("ln -s $public_prod_downloads/$report_name $public_dev_downloads/$report_name");
	}

	if (-e "$public_prod_downloads/$report_name") {
		return {
			result =>{
				filename => "/downloads/$report_name",
				done  => 1
			}
		};	
	} else {
		return api_error("Unable to generate a Report.");
	}

};

get '/custom_reports/config' => sub {

	$config->{custom_roamer_report_properties} = get_custom_roamers_report_properties() if defined &get_custom_roamers_report_properties;
	$config->{custom_roamer_report_properties} ||= { subscription => $cache->{rt_subscription} };

	$cache->{rt_subscription} ||= get_table_hash($dbh, 'RT_SUBSCRIPTION', {id => 'ID', cols => 'NAME'});

	my $result = {
		roamer_types => $roaming_types,
		current_index => Invigo::elastic_index( $config->{smsw_current}),
		properties => $config->{custom_roamer_report_properties}
	};

	$result->{countries} = $cache->{interface_cache}->{countries} if $countries_changed;
	$result->{operators} = $cache->{interface_cache}->{operators} if $operators_changed;

	return $result;

};

#######################
# BULK CAMPAIGNS APIS #
#######################

get '/bulk_campaigns' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access('/bulk/campaigns', 'V'));
	my ($user, $user_info, $user_id, $is_admin, $has_access);

	# Get user and access data
	$user = session('username');
	$user_info = get_user_info($user);
	$user_id = $user_info->{ID};
	$is_admin = $user_info->{GROUPID} eq '1';
	$has_access = get_value_from_db($dbh, "SELECT COUNT(*) FROM TBL_BULK_USERS WHERE USER_ID=?", $user_id);
	
	# Check if user has access bulk campaigns
	if ($has_access < 1) {
		$has_access = get_value_from_db($dbh, "SELECT COUNT(*) FROM TBL_BULK_USERS WHERE USER_ID=0 AND GROUP_ID=(SELECT ID FROM ADM_GROUPS WHERE NAME=?)", $user_info->{GROUPID});
	}
	return api_error('NOT_ALLOWED_TO_MANAGE_BULK_CAMPAIGNS') unless $has_access;

	my ($days_limit, $date_limit, $creation_date, $limit_user_view);
	my ($campaigns, $result, $access, $sending_hours, $bulk_messages);

	# Get current campaigns from db
	$days_limit = get_config_value('BULK_CAMPAIGNS_DAYS_LIMIT', 60);
	$date_limit = get_sql('date_sub_days', 'sysdate', $days_limit);
	$creation_date = get_sql('date_to_char', 'C.CREATION_DATE', 'YYYY-MM-DD HH24:MI:SS');
	$limit_user_view = ($is_admin ? "" : " C.USER_ID=$user_id AND ");
	$campaigns = get_rows_from_db($dbh, "SELECT C.ID, C.TITLE, $creation_date, C.STATUS, C.PROCESSED_COUNT, C.TOTAL_COUNT, M.NAME FROM TBL_BULK_CAMPAIGNS C, TBL_BULK_MESSAGES M WHERE $limit_user_view CREATION_DATE>=$date_limit AND STATUS<100 AND C.MESSAGE_ID=M.ID ORDER BY CREATION_DATE DESC");

	# Get access, sending hours, and messages data
	$access = has_access('/bulk/campaigns');
	$sending_hours = get_table_hash($dbh, 'TBL_BULK_HOURS', {id => 'ID', cols => 'DESCRIPTION'});
	$bulk_messages = get_table_hash($dbh, 'TBL_BULK_MESSAGES', {id => 'ID', cols => 'NAME'});

	$result = {rows => $campaigns, access => $access, sending_hours => $sending_hours, messages => $bulk_messages};
	return $result;
};
  
get '/bulk_campaigns/:campaign_id' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access('/bulk/campaigns', 'V'));
	return api_error('INVALID_OR_MISSING_CAMPAIGN_ID') unless (params->{campaign_id});

	my ($campaign_id, $with_stats, $info);

	# Get specific campaign info
	$campaign_id = params->{campaign_id};
	$with_stats = params->{with_stats};
	$info = get_bulk_campaign_info($campaign_id, $with_stats);

	return {result => $info};
};

any ['post', 'put'] => '/bulk_campaigns/all/:action' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless has_access('/bulk/campaigns' ,'E');

	my $action = params->{action};
	return api_error('INVALID_BULK_ACTION') unless ($action eq 'pause' || $action eq 'resume');

	# Pause or resume all current campaigns
	my $result = pause_resume_all_campaigns($action, 'bulk');
	if (!$result->{result}){
		return api_error('COULD_NOT_'.uc($action).'_ALL_CAMPAIGN');
	}
	return {result => "OK"};
};

any ['post', 'put'] => '/bulk_campaigns/update/:action' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless has_access('/bulk/campaigns' ,'E');
	return api_error('INVALID_OR_MISSING_CAMPAIGN_ID') unless (params->{campaign_id});
	
	my ($action, $id, $result);

	# Get action and campaign id from request
	$action = params->{action};
	return api_error('INVALID_BULK_ACTION') unless ($action eq 'pause' || $action eq 'resume' || $action eq 'delete' || $action eq 'cancel');
	$id = params->{campaign_id};

	# Perform specific action on camapign
	if ($action eq 'pause' || $action eq 'resume') {
		$result = pause_resume_campaign($id, $action, 'bulk');
		return api_error('COUNT_NOT_'.uc($action).'_BULK_CAMPAIGN') if (!$result->{result});
	} elsif ($action eq 'cancel') {
		$result = cancel_campaign($id, 'bulk');
		return api_error('COUNT_NOT_CANCEL_BULK_CAMPAIGN') if (!$result->{result});
	} elsif ($action eq 'delete') {
		$result = execute_sql($dbh, "UPDATE TBL_BULK_CAMPAIGNS SET STATUS=STATUS+100 WHERE ID=? AND STATUS>0 AND STATUS<100", $id);
		return api_error('COULD_NOT_DELETE_BULK_CAMPAIGN') if (!$result);
		
		$result = {status => 100};
	}

	return {result => "OK", status => $result->{status}};
};

any ['post', 'put'] => '/bulk_campaigns/create' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless has_access('/bulk/campaigns', 'I');
	my ($title, $dates, $require_report, $sending_hours, $message, $created_cid);

	# Get params from request
	$title = params->{title};
	$dates = params->{dates};
	$require_report = params->{require_report};
	$sending_hours = params->{sending_hours};
	$message = params->{message};

	# Check if all params are provided
	return api_error('MISSING_OR_INVALID_TITLE') if (!$title);
	return api_error('MISSING_OR_INVALID_DATES') if (!$dates);
	return api_error('MISSING_OR_INVALID_SENDING_HOURS') if (!$sending_hours);
	return api_error('MISSING_OR_INVALID_MESSAGE') if (!$message);

	my ($start_date, $end_date);

	$start_date = $dates->[0];
	$end_date = $dates->[1];

	# Create new campaign
	my $res = create_campaign($title, $start_date, $end_date, $require_report, $sending_hours, $message);
	if ($res->{'error'}) {
		execute_sql("DELETE FROM TBL_BULK_CAMPAIGNS WHERE ID=?", $res->{id});
		return {result => $res->{'error'}};
	}
	$created_cid = $res->{id};

	return {result => "OK", cid => $created_cid};
};

post '/bulk_campaigns/remove' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless has_access('/bulk/campaigns', 'I');
	return api_error('MISSING_OR_INVALID_PARAMS') if (!params->{cid});

	# Delete campaign from db
	my $cid = params->{cid};
	execute_sql("DELETE FROM TBL_BULK_CAMPAIGNS WHERE ID=?", $cid);

	return {result => "OK"};
};

post '/move_uploaded_file' => sub { 
	return api_error('ACTION_NOT_ALLOWED') unless has_access('/bulk/campaigns', 'I');
	return api_error('MISSING_OR_INVALID_PARAMS') if (!params->{cid} || !params->{filename});

	my ($uploaded_filename, $cid, $remove_duplicates);

	# Get params from request
	$uploaded_filename = $config->{UPLOAD_FOLDER}."/".params->{filename};
	$cid = params->{cid};
	$remove_duplicates = (defined(params->{remove_duplicates}) ? params->{remove_duplicates} : 0);

	# Check if should remove duplicates from file
	if ($remove_duplicates) {
		my $tmp_file = $config->{BULK_FOLDER}."/tmp_".params->{cid}.".txt";
		`dos2unix $uploaded_filename; sort $uploaded_filename | uniq > $tmp_file`;
		$uploaded_filename = $tmp_file;
	}

	# Move uploaded file into bulk folder
	copy($uploaded_filename, $config->{BULK_FOLDER}."/Bulk_".$cid.".txt") or return api_error("COULD_NOT_COPY_FILE");
	unlink($uploaded_filename);

	return {result => "OK"};
};

get '/download_bulk_file/:filename' => sub {
	my $filename  = request->params->{filename} or return api_error('INVALID_OR_MISSING_FILENAME');
	my $file = path($config->{BULK_FOLDER}, $filename);

	return send_file($file, system_path => 1, content_disposition => 'attachement', content_type => 'text/csv', filename => $filename);
};

get '/bulk_hours' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access('/bulk/hours', 'V'));
	my ($access, $user, $user_info, $user_id, $rows);

	# Get user and access info
	$access = has_access('/bulk/hours');
	$user = session('username');
	$user_info = get_user_info($user);
	$user_id = $user_info->{ID};
	
	# Get sending hours from db
	$rows = get_rows_from_db($dbh, "SELECT ID, DESCRIPTION FROM TBL_BULK_HOURS ORDER BY ID");

	return {rows => $rows, access => $access};
};

get '/bulk_hours/:sending_hours_id' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access('/bulk/hours', 'V'));
	my ($sending_hours_id, $rows);

	# Get specific sending hours from db
	$sending_hours_id  = request->params->{sending_hours_id} or return api_error('INVALID_OR_MISSING_ID');
	$rows = get_rows_from_db($dbh, "SELECT MON, TUE, WED, THU, FRI, SAT, SUN, DESCRIPTION FROM TBL_BULK_HOURS WHERE ID=?", $sending_hours_id);
	
	return {result => $rows->[0]};
};

any['post' , 'put'] => '/bulk_hours/create' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless has_access('/bulk/hours' , 'I');
	my ($user, $user_info, $user_id);
	my ($name, $selected_hours, $name_already_exists);
	my ($sql, @sql_vars, $res);
	my @week_days = ('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun');

	# Get user and access info
	$user = session('username');
	$user_info = get_user_info($user);
	$user_id = $user_info->{ID};

	# Get params from request
	$name = request->params->{name} or return api_error('INVALID_OR_MISSING_NAME');
	$selected_hours = request->params->{selected_hours} or return api_error('INVALID_OR_MISSING_HOURS');
	
	# Check if sending hours name already exists
	$name_already_exists = get_value_from_db($dbh, "SELECT DESCRIPTION FROM TBL_BULK_HOURS WHERE DESCRIPTION=?", $name);
	return api_error('NAME_ALREADY_IN_USE') if $name_already_exists;

	# Construct sql
	$sql = "INSERT INTO TBL_BULK_HOURS (ID, USER_ID, DESCRIPTION";
	foreach my $day (@week_days) {
		$sql .= ", ".(uc $day);
	}
	$sql .= ") VALUES (".get_sql('nextval', 'SEQ_BULK_HOURS').",?,?";
	
	@sql_vars = ($user_id, $name);
	foreach my $day (@week_days) {
		$sql .= ", ?";
		push(@sql_vars, (defined($selected_hours->{$day}) ? $selected_hours->{$day} : ""));
	}
	$sql .= ")";

	# Insert sending hours into db
	$res = execute_sql($dbh, $sql, @sql_vars);
	if ($res) {
		Invigo::set_db_update_time();
	} else {
		return api_error('COULD_NOT_CREATE_BULK_HOURS') ;
	}

	return {result => "OK"};
};

any['post' , 'put'] => '/bulk_hours/update/:sending_hours_id' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless has_access('/bulk/hours' , 'E');
	my ($sending_hours_id, $name, $selected_hours);
	my ($user, $user_info);
	my ($sql, @sql_vars, $res);
	my @week_days = ('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun');

	# Get params from request
	$sending_hours_id = request->params->{sending_hours_id} or return api_error('INVALID_OR_MISSING_ID');
	$name = request->params->{name} or return api_error('INVALID_OR_MISSING_NAME');
	$selected_hours = request->params->{selected_hours} or return api_error('INVALID_OR_MISSING_HOURS');

	# Construct sql
	$sql = "UPDATE TBL_BULK_HOURS SET DESCRIPTION=?";
	@sql_vars = ($name);
	foreach my $day (@week_days) {
		$sql .= ", ".(uc $day)." = ?";
		push(@sql_vars, (defined($selected_hours->{$day}) ? $selected_hours->{$day} : ""));
	}
	$sql .= " WHERE ID=?";
	push(@sql_vars, $sending_hours_id);

	# Get user and access info
	$user = session('username');
	$user_info = get_user_info($user);
	if ($user_info->{GROUPID} ne '1') {
		$sql .= " AND USER_ID=?";
		push(@sql_vars, $user_info->{ID});
	}
	
	# Update sending hours in db
	$res = execute_sql($dbh, $sql, @sql_vars);
	if ($res == 1) {
		Invigo::set_db_update_time();
	} else {
		return api_error('COULD_NOT_UPDATE_BULK_HOURS') ;
	}

	return {result => "OK"};
};

any['post' , 'put'] => '/bulk_hours/delete' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless has_access('/bulk/hours' , 'D');
	my ($id, $sql, @sql_vars, $user, $user_info, $res);

	# Get id from request and construct sql
	$id  = request->params->{id} or return api_error('INVALID_OR_MISSING_ID');
	$sql = "DELETE FROM TBL_BULK_HOURS WHERE ID=?";
	@sql_vars = ($id);

	# Get user and access info
	$user = session('username');
	$user_info = get_user_info($user);
	if ($user_info->{GROUPID} ne '1') {
		$sql .= " AND USER_ID=?";
		push(@sql_vars, $user_info->{ID});
	}

	# Delete sending hours from db
	$res = Invigo::execute_sql($dbh, $sql, @sql_vars);
	if ($res == 1) {
		Invigo::set_db_update_time();
	} else {
		return api_error('COULD_NOT_DELETE_BULK_HOURS') ;
	}

	return {result => "OK"};
};

##################
# SIMULATOR APIS #
##################

get '/simulator/init_simulator_data' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access('/simulator'));

	my ($rt_operators, $rt_countries, $simulator_config, $operator_hni, $country_mcc, $events);
	
	$operator_hni =  get_config_value('OPERATOR_HNI');

	my $country_prefix = get_config_value('COUNTRY_PREFIX');
	my $code_to_mcc = get_table_hash($dbh, 'RT_COUNTRY_CODES', {id => 'CODE', cols => 'MCC'});

	$country_mcc  =  $code_to_mcc->{$country_prefix};

	# Get countries that have operators defined
	$rt_countries = $cache->{rt_countries};
	while (my ($mcc, $name) = each %{$cache->{rt_countries}}) {
		delete($rt_countries->{$mcc}) if (!defined($cache->{mcc_hni_operators}->{$mcc}));
		delete($rt_countries->{$mcc}) if (index($name, 'DO_NOT_USE') != -1);
	}

	# Check if need to manage cell id
	$simulator_config->{manage_cell_id} = ($config->{manage_cell_id_in} || $config->{manage_cell_id_out});
	$simulator_config->{full_simulation_enabled} = get_config_value("FULL_SIMULATOR_ENABLED",0);
	$events={'ROAM' => 'ROAM', 'BACK' => 'BACK', 'SRI' => 'SRI', 'GPRSROAM'=>'GPRSROAM','LOCAL'=>'LOCAL'};
	$events->{'BACK_CONFIRM'}='BACK_CONFIRM' if ($config->{send_back_sri});

	my @cache_to_clear = qw(RECENT_MAP_MESSAGES RELATED_IMSI_MAP_MESSAGES TBL_SUBSCRIBERS_INFO TBL_WINBACK_TIMESTAMP TBL_IN_BACK_TEMP TBL_MESSAGES_SENT TBL_MULTI_DEVICE_IMSI_DETECTIONS);
	
	return { cache_values=> \@cache_to_clear, countries => $rt_countries, operators => $cache->{mcc_hni_operators}, config => $simulator_config, events=>$events, operator_hni => $operator_hni, country_mcc => $country_mcc };

};

get '/simulator/get_current_location/:msisdn' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access('/simulator'));
	return api_error('MISSING_ROAMER_IDENTIFICATION') if (!params->{msisdn});

	my ($row, $current_location, $id, $source);
	
	$id = params->{msisdn} || "NULL";
	$source = undef;
	# $source = "visited_country_name,visited_operator_name";

	if($config->{db_backend_enabled}){	
		$row = get_rows_from_db($dbh, "SELECT VIS_COUNTRY,VIS_OPERATOR FROM TBL_ROAMERS WHERE MSISDN=?", $id);
	}else{
		my $query = {
			bool => {
				should => [
					{ 
						match => {
							msisdn => $id
						}
					},
					{ 
						match => { 
							imsi => $id
						} 
					}
				],
				must_not => [
					{	
						exists => {
							field=> "date_out"
						}
					}
				],
				minimum_should_match => 1
			}
		};

		@$row = Invigo::Elastic::dump_index($config->{smsw_current},$source,$query, undef,[{ date_last_seen => {order => 'asc'}},{ date_out => {order => 'desc'}},{ date_in => {order => 'desc'}}]);

	}

	if ($row && $row->[0]) {
		$row = $row->[0];
		$current_location = $config->{db_backend_enabled} ? $cache->{rt_countries}->{$row->[0]}." - ".$cache->{rt_operators}->{$row->[1]} : $row->{visited_operator_name}." - ".$row->{visited_country_name};
	}

	return {current_location => $current_location};
};

any ['post', 'put'] => '/simulator/send_msisdn/:msisdn' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access('/simulator'));
	return api_error("BAD_PARAMETER") if !defined(params->{map_message}) || params->{map_message} !~ /^(ROAM|SRI|LOCAL|GPRSROAM|BACK|BACK_CONFIRM)$/;
	return api_error('MISSING_ROAMER_IDENTIFICATION') if (!params->{msisdn});

	my ($mcc, $hni, $vlr, $imsi, $date, $map_message, $msisdn);
	my ($row, $result, $roamer, $vlr_length, $extra);

	$msisdn = params->{msisdn};
	$map_message = params->{map_message};
	$extra = params->{extra_opts};

	# Decode extra options
	$extra = decode_json($extra) if defined($extra);
	$extra->{manage_cell_id} = ($config->{manage_cell_id_in} || $config->{manage_cell_id_out});

	# Get params from request
	$vlr = params->{vlr} if defined params->{vlr};
	$imsi = defined($extra->{imsi}) ? $extra->{imsi} : "";
	my ($type,$mismatch) = get_roamer_type({msisdn=>$msisdn , imsi=>$imsi , map_message=>$map_message});
	if (!$type || ($mismatch && $type eq 'O' && !$config->{manage_multi_imsi_out}) || ($mismatch && $type eq 'I' && !$config->{manage_multi_imsi_in})) {
		return api_error("MISMATCH_BETWEEN_MSISDN_".$msisdn."_AND_IMSI_$imsi")
	}

	if(!defined params->{vlr}){

		$mcc = params->{mcc} or return api_error('INVALID_OR_MISSING_MCC');
		$hni = params->{hni} or return api_error('INVALID_OR_MISSING_HNI');
		
		# Generate vlr
		$vlr = (defined($cache->{operator_prefixes}->{$hni}) && $cache->{operator_prefixes}->{$hni} ? ($cache->{operator_prefixes}->{$hni}->{PREFIXES}->[int(rand(scalar @{$cache->{operator_prefixes}->{$hni}->{PREFIXES}}))]): $cache->{rt_country_codes}->{$mcc});
		return api_error('VLR_COULD_NOT_BE_GENERATED') if (!$vlr);
		$vlr_length = length($vlr) + 6;
		while (length($vlr) < $vlr_length) {
			$vlr .= (int(rand(10)));
		}
	}
	return api_error('ROAMER_IS_NOT_LOCAL') if (!(number_is_local($msisdn) && number_is_local($vlr)) && uc $map_message eq 'LOCAL');

	# Get imsi and roamer type from msisdn
	if (!$imsi){
		if($config->{db_backend_enabled}){
			
			$row = get_rows_from_db($dbh, "SELECT TYPE,IMSI FROM TBL_ROAMERS WHERE MSISDN=?", $msisdn);

		} else{

			my $query = {
				bool => {
					must => [
						{ 
							match => { 
								msisdn => $msisdn
							} 
						}
					],
					must_not => [
						{	
							exists => {
								field => "date_out"
							}
						}
					]
				},
			};

			my $source = "imsi,roamer_type";
			@$row = eval { Invigo::Elastic::dump_index( $config->{smsw_current}, $source, $query, undef, [{ date_last_seen => { order => 'desc' }}]); };

		}
	
		$cache->{roamer_types} ||= Invigo::get_table_hash($dbh, 'RT_INOUT', {id => 'ID', cols => 'NAME'});
		if ($row && ((ref($row) eq 'HASH' && %$row) || (ref($row) eq 'ARRAY' && $row->[0]))){
			$row = $row->[0] if ref($row) eq 'ARRAY' && $row->[0] ;
			$row = [$row->{roamer_type},$row->{imsi}] unless $config->{db_backend_enabled};
			$type = $row->[0];
			($type) = grep { $cache->{roamer_types}->{$_} eq $row->[0] } keys %{$cache->{roamer_types}} unless $config->{db_backend_enabled};
			$imsi = defined($extra->{imsi}) ? $extra->{imsi} : $row->[1];
		} else {
			$type = "O";
		}
	}

	return api_error("ROAMER_TYPE_NOT_FOUND") if (!$type);

	# Set current date and roamer params
	$date = Invigo::get_date_time_str({date_separator => '-', time_separator => ':', date_time_separator => ' '});
	$roamer = {type => $type, msisdn => $msisdn, vlr => $vlr, imsi => $imsi, event_date => $date, map_message => $map_message};

	# Add to smswelcome queue
	if ($config->{send_back_sri} && $map_message eq 'BACK_CONFIRM'){
		$roamer->{map_message}='BACK';
		$extra->{send_back_confirmation}=1;
	}
	$result = $add_to_mapfilter->($dbh, $roamer, $extra);

	return api_error('OPERATION_FAILED') if ($result eq "ERROR");
	return {result => $result};
};

any ['post', 'put'] => '/simulator/delete_msisdn/:msisdn' => sub {
	
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'D') || has_access('/simulator'));
	return api_error('MISSING_OR_BAD_ROAMER_IDENTIFICATION') if !params->{msisdn} || params->{msisdn} !~ /^\d+$/;

	my $sqls_to_execute = [];
	my $roamers_tables_list = ['TBL_MAPFILTER','TBL_SMS_SENT_DUMMY','TBL_SMS_SENT_IN','TBL_SMS_SENT_OUT','TBL_SMS_SENT_WINBACK','TBL_SMS_SENT_LOCAL','TBL_SMS_SENT_THANKYOU','TBL_IN_BACK_TEMP'];
	push @$roamers_tables_list, ('TBL_ROAMERS_I', 'TBL_ROAMERS_O', 'TBL_ROAMERS_L','TBL_ROAMERS_N','TBL_STATISTICS') if $config->{db_backend_enabled};
	push @$sqls_to_execute, ["DELETE FROM $_ WHERE MSISDN = ?",params->{msisdn}] foreach (@$roamers_tables_list);
	my $res=Invigo::execute_sqls($dbh,@$sqls_to_execute);
	return api_error("ROAMER_DELETE_FAILED") unless $res;

	if($config->{elastic}){
		my $indexes = Invigo::get_config_value('ELASTIC_STATS_INDEXES',"$config->{smsw_sms_events}, $config->{smsw_current},$config->{smsw_trips}");
		$config->{elastic}->delete_by_query(index => Invigo::elastic_index($_), body => {query => {match=>{msisdn=>params->{msisdn}}}}) foreach split(",",$indexes);
	}

	return {result => "OK"};	 
};

any ['post', 'put'] => '/simulator/clear_cache' => sub {
	
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'D') || has_access('/simulator'));

	my $params = request->body;

	#JSON Decode our params to a hash
	if (ref($params) ne 'HASH' && $params =~ /^\[/) {
    	$params = decode_json($params);
	}

	#Boolean to track if we have errors in clearing cache
	my $clear_cache = 1;

	#Check if we have an array of cache keys to clear
	if($params->[0]->{cache}){

		#Loop over array values and clear the corresponding caches
		foreach my $cache_key (@{$params}){
			$clear_cache &= $config->{InvigoCache}->clear($cache_key->{cache}) if $cache_key->{cache}; #only clear cache if actual value is supplied
			if (!$clear_cache){
				log_error("Could not clear cache for " . $cache_key->{cache});
			}
		}
	}else{
		#Clear all caches if none are specified
		$clear_cache = $config->{InvigoCache}->clear('recent_map_messages') && 
			$config->{InvigoCache}->clear('related_imsi_map_messages') && 
			$config->{InvigoCache}->clear('tbl_subscribers_info') && 
			$config->{InvigoCache}->clear('tbl_winback_timestamp') &&
			$config->{InvigoCache}->clear('tbl_in_back_temp') &&
			$config->{InvigoCache}->clear('TBL_MESSAGES_SENT') &&
			$config->{InvigoCache}->clear('tbl_multi_device_imsi_detections');
	}
	


	return {result => "OK"} if $clear_cache;
	return api_error("ONE OR MORE CACHE CLEAR FAILED");
};

any ['post','put'] => '/simulator/delete_all_msisdns' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'A') || has_access('/simulator'));
	my $sqls_to_execute = [];
	my $roamers_tables_list = ['TBL_MAPFILTER','TBL_SMS_SENT_DUMMY','TBL_SMS_SENT_IN','TBL_SMS_SENT_OUT','TBL_SMS_SENT_WINBACK','TBL_SMS_SENT_LOCAL','TBL_SMS_SENT_THANKYOU','TBL_IN_BACK_TEMP'];
	push @$roamers_tables_list, ('TBL_ROAMERS_I', 'TBL_ROAMERS_O', 'TBL_ROAMERS_L','TBL_ROAMERS_N','TBL_STATISTICS') if $config->{db_backend_enabled};
	push @$sqls_to_execute, ["DELETE FROM $_ "] foreach (@$roamers_tables_list);
	my $res=Invigo::execute_sqls($dbh,@$sqls_to_execute);
	return api_error("Delete Failed") unless $res;
	if($config->{elastic}){
		my $indexes = Invigo::get_config_value('ELASTIC_STATS_INDEXES',"$config->{smsw_sms_events}, $config->{smsw_current},$config->{smsw_trips}");
		$config->{elastic}->delete_by_query(index => Invigo::elastic_index($_), body => {query => {match_all=>{}}}) foreach split(",",$indexes);
	}
	return {result => "OK"};	 
};

any ['post', 'put'] => '/simulator/start_full_simulation' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access('/simulator'));

	my ($orig_mcc, $orig_hni, $num_inbound, $num_outbound, $inbound_percentages, $outbound_percentages);
	my ($in_to_generate, $out_to_generate, @in_out);
	my ($inbound_info, $outbound_info) = ({}, {});
	my ($in_insert_count, $out_insert_count) = (0, 0);

	# Get params from request
	$orig_mcc = params->{orig_mcc} or return api_error('INVALID_OR_MISSING_ORIGINATING_MCC');
	$orig_hni = params->{orig_hni} or return api_error('INVALID_OR_MISSING_ORIGINATING_HNI');
	$num_inbound = (defined(params->{num_inbound}) ? params->{num_inbound} : 0);
	$num_outbound = (defined(params->{num_outbound}) ? params->{num_outbound} : 0);
	return api_error('INVALID_OR_MISSING_ROAMERS_NUMBER') if (($num_inbound + $num_outbound) > 2000000 || ($num_inbound + $num_outbound) < 1);
	$inbound_percentages = params->{inbound_percentages};
	$outbound_percentages = params->{outbound_percentages};
	return api_error('INVALID_OR_MISSING_ROAMERS_LOCATION_INFO') if (($num_inbound && !$inbound_percentages) || ($num_outbound && !$outbound_percentages));

	# Decode percentages
	$inbound_percentages = decode_json($inbound_percentages) if ($inbound_percentages);
	$outbound_percentages = decode_json($outbound_percentages) if ($outbound_percentages);

	# Get inbound info
	if ($num_inbound > 0) {
		foreach my $country (@$inbound_percentages) {
			my ($selected_hni, $operators_list, @keys);

			next if (!$country->{percentage}) || !$cache->{rt_countries}->{$country->{selected_mcc}}; 

			# Get random operator if none is selected
			$selected_hni = $country->{selected_hni};
			if (!$selected_hni) {
				$operators_list = $cache->{mcc_hni_operators}->{$country->{selected_mcc}};
				@keys = keys(%$operators_list);
				$selected_hni = $keys[rand @keys];
			}
			$inbound_info->{$selected_hni}->{mcc} = $country->{selected_mcc};

			# Get number from percentage
			$inbound_info->{$selected_hni}->{num_to_generate} = floor(($country->{percentage}/100)*$num_inbound);
		}
	}

	# Get outbound info
	if ($num_outbound > 0) {
		foreach my $country (@$outbound_percentages) {
			my ($selected_hni, $operators_list, @keys);

			next if (!$country->{percentage}) || !$cache->{rt_countries}->{$country->{selected_mcc}}; 

			# Get random operator if none is selected
			$selected_hni = $country->{selected_hni};
			if (!$selected_hni) {
				$operators_list = $cache->{mcc_hni_operators}->{$country->{selected_mcc}};
				@keys = keys(%$operators_list);
				$selected_hni = $keys[rand @keys];
			}
			$outbound_info->{$selected_hni}->{mcc} = $country->{selected_mcc};

			# Get number from percentage
			$outbound_info->{$selected_hni}->{num_to_generate} = floor(($country->{percentage}/100)*$num_outbound);
		}
	}

	# Initialize roamers data
	$in_to_generate = $num_inbound;
	$out_to_generate = $num_outbound;
	@in_out = ('I', 'O');
	my $roamers={};

	# Generate roamers
	for (my $i=0; $i < ($num_inbound + $num_outbound); $i++) {
		my (@info_keys, $home_mcc, $home_hni, $home_country_code, $vis_mcc, $vis_hni, $vis_country_code);
		my ($vlr_length, $msisdn_length, $imsi_length, $vlr, $imsi, $msisdn, $date, $roamer);
		my $direction = ($in_to_generate && $out_to_generate ? $in_out[rand @in_out] : ($in_to_generate ? 'I' : 'O'));

		# Check which type of roamer we're generating
		if ($direction eq 'I') {
			# Set home location info
			@info_keys = keys(%$inbound_info);
			$home_hni = $info_keys[rand @info_keys];
			$home_mcc = $inbound_info->{$home_hni}->{mcc} if defined($home_hni) && defined($inbound_info->{$home_hni}->{mcc});
			$home_country_code = $cache->{rt_country_codes}->{$home_mcc} if defined($home_hni) && defined($cache->{rt_country_codes}->{$home_hni});

			# Set visited location info
			$vis_hni = $orig_hni;
			$vis_mcc = $orig_mcc;
			$vis_country_code = $cache->{rt_country_codes}->{$vis_mcc};

			next if !defined($home_hni) || !defined $inbound_info->{$home_hni} || !$inbound_info->{$home_hni} || !$inbound_info->{$home_hni}->{mcc} || $inbound_info->{$home_hni}->{num_to_generate} lt 1;

			# Decrement roamer numbers
			$in_to_generate--;
			$inbound_info->{$home_hni}->{num_to_generate}--;
			delete($inbound_info->{$home_hni}) if ($inbound_info->{$home_hni}->{num_to_generate} == 0);
		} elsif ($direction eq 'O') {
			# Set home location info
			$home_hni = $orig_hni;
			$home_mcc = $orig_mcc;
			$home_country_code = $cache->{rt_country_codes}->{$home_mcc};

			# Set visited location info
			@info_keys = keys(%$outbound_info);
			$vis_hni = $info_keys[rand @info_keys];
			$vis_mcc = $outbound_info->{$vis_hni}->{mcc} if defined($vis_hni) && defined($outbound_info->{$vis_hni}->{mcc});
			$vis_country_code = $cache->{rt_country_codes}->{$vis_mcc} if defined($vis_mcc) && defined($cache->{rt_country_codes}->{$vis_mcc});

			next if !defined($vis_hni) || !defined $outbound_info->{$vis_hni} || !$outbound_info->{$vis_hni} || !$outbound_info->{$vis_hni}->{mcc} || $outbound_info->{$vis_hni}->{num_to_generate} lt 1;

			# Decrement roamer numbers
			$out_to_generate--;
			$outbound_info->{$vis_hni}->{num_to_generate}--;
			delete($outbound_info->{$vis_hni}) if ($outbound_info->{$vis_hni}->{num_to_generate} == 0);
		}

		# Generate vlr
		$vlr = (defined($cache->{operator_prefixes}->{$vis_hni}) && $cache->{operator_prefixes}->{$vis_hni} ? ($cache->{operator_prefixes}->{$vis_hni}->{PREFIXES}->[int(rand(scalar @{$cache->{operator_prefixes}->{$vis_hni}->{PREFIXES}}))]) : $vis_country_code);
		$vlr_length = length($vlr) + 6;
		while (length($vlr) < $vlr_length) {
			$vlr .= (int(rand(10)));
		}

		# Generate imsi
		$imsi = $home_hni;
		while (length($imsi) < 15) {
			$imsi .= (int(rand(10)));
		}

		# Generate msisdn
		$msisdn = (defined($cache->{operator_prefixes}->{$home_hni}) && $cache->{operator_prefixes}->{$home_hni} ? ($cache->{operator_prefixes}->{$home_hni}->{PREFIXES}->[int(rand(scalar @{$cache->{operator_prefixes}->{$home_hni}->{PREFIXES}}))]) : $home_country_code);
		next if not defined $msisdn;
		$msisdn_length = 15-length($msisdn);
		for(1..$msisdn_length) {
			$msisdn .= (int(rand(10)));
		}

		# Set current date and roamer params
		$date = Invigo::get_date_time_str({date_separator => '-', time_separator => ':', date_time_separator => ' '});
		$roamer = {type => $direction, msisdn => $msisdn, vlr => $vlr, imsi => $imsi, event_date => $date, map_message => 'ROAM'};

		$roamers->{$msisdn}=$roamer; #key is msisdn since it is built based on prefix....
	}
	foreach (keys %$roamers){
		# Add to smswelcome queue
		my ($roamer, $result);
		$roamer=$roamers->{$_};
		$result = $add_to_mapfilter->($dbh, $roamer, {});
		$in_insert_count++ if ($roamer->{type} eq 'I' && $result eq "OK");
		$out_insert_count++ if ($roamer->{type} eq 'O' && $result eq "OK");

	}

	return {result => "OK", total_roamers => ($in_insert_count+$out_insert_count), inbound_roamers => $in_insert_count, outbound_roamers => $out_insert_count, x=>$roamers};
};

############################
# MARKETING CAMPAIGNS APIS #
############################

get '/marketing_campaigns' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access('/marketing_campaigns', 'V'));

	my ($days_limit, $date_limit, $creation_date);
	my ($campaigns, $result, $access, $messages);

	# Update campaign statuses
	execute_sql($dbh, "UPDATE TBL_MARKETING_CAMPAIGNS SET STATUS=? WHERE STATUS=? AND ".get_sql('sysdate').">=START_DATE AND ".get_sql('sysdate')."<END_DATE", 1, 0);
	execute_sql($dbh, "UPDATE TBL_MARKETING_CAMPAIGNS SET STATUS=? WHERE STATUS < 3 AND ".get_sql('sysdate').">=END_DATE", 3);

	# Get current campaigns from db
	$days_limit = get_config_value('MARKETING_CAMPAIGNS_DAYS_LIMIT', 60);
	$date_limit = get_sql('date_sub_days', 'sysdate', $days_limit);
	$creation_date = get_sql('date_to_char', 'C.CREATION_DATE', 'YYYY-MM-DD HH24:MI:SS');
	$campaigns = get_rows_from_db($dbh, "SELECT C.ID, C.TITLE, $creation_date, C.STATUS, M.INFO FROM TBL_MARKETING_CAMPAIGNS C, TBL_ALL_MESSAGES M WHERE CREATION_DATE>=$date_limit AND STATUS<100 AND C.MESSAGE_ID=M.ID ORDER BY CREATION_DATE DESC");

	# Get access, sending hours, and messages data
	$access = has_access('/marketing_campaigns');
	$messages=Invigo::get_table_hash('tbl_all_messages' , {id=>'ID' , cols=>'INFO' });

	my @event_types= sort keys %{$event_type_to_state_hash};

	$result = {rows => $campaigns, access => $access, messages => $messages, message_types => \@event_types, states => $cache->{rt_visitingback}, regions => $cache->{tbl_region}, countries => $cache->{rt_countries}, operators => $cache->{rt_operators}, subscriptions => $cache->{rt_subscription}};
	return $result;
};

get '/marketing_campaigns/:campaign_id' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access('/marketing_campaigns', 'V'));
	return api_error('INVALID_OR_MISSING_CAMPAIGN_ID') unless (params->{campaign_id});

	my ($campaign_id, $info);

	# Get specific campaign info
	$campaign_id = params->{campaign_id};
	$info = get_marketing_campaign_info($campaign_id);

	return {result => $info};
};



any ['post', 'put'] => '/marketing_campaigns/all/:action' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless has_access('/marketing_campaigns' ,'E');

	my $action = params->{action};
	return api_error('INVALID_MARKETING_CAMPAIGNS_ACTION') unless ($action eq 'pause' || $action eq 'resume');

	# Pause or resume all current campaigns
	my $result = pause_resume_all_campaigns($action, 'marketing');
	if (!$result->{result}){
		return api_error('COULD_NOT_'.uc($action).'_ALL_CAMPAIGN');
	}
	return {result => "OK"};
};

any ['post', 'put'] => '/marketing_campaigns/update/:action' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless has_access('/marketing_campaigns' ,'E');
	return api_error('INVALID_OR_MISSING_CAMPAIGN_ID') unless (params->{campaign_id});
	
	my ($action, $id, $result);

	# Get action and campaign id from request
	$action = params->{action};
	return api_error('INVALID_MARKETING_CAMPAIGNS_ACTION') unless ($action eq 'pause' || $action eq 'resume' || $action eq 'delete' || $action eq 'cancel');
	$id = params->{campaign_id};

	# Perform specific action on camapign
	if ($action eq 'pause' || $action eq 'resume') {
		$result = pause_resume_campaign($id, $action, 'marketing');
		return api_error('COUNT_NOT_'.uc($action).'_MARKETING_CAMPAIGN') if (!$result->{result});
	} elsif ($action eq 'cancel') {
		$result = cancel_campaign($id, 'marketing');
		return api_error('COUNT_NOT_CANCEL_MARKETING_CAMPAIGN') if (!$result->{result});
	} elsif ($action eq 'delete') {
		$result = execute_sql($dbh, "UPDATE TBL_MARKETING_CAMPAIGNS SET STATUS=STATUS+100 WHERE ID=? AND STATUS>0 AND STATUS<100", $id);
		return api_error('COULD_NOT_DELETE_MARKETING_CAMPAIGN') if (!$result);
		
		$result = {status => 100};
	}

	return {result => "OK", status => $result->{status}};
};

any ['post', 'put'] => '/marketing_campaigns/create' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless has_access('/marketing_campaigns', 'I');
	my ($title, $dates, $message, $created_cid, $type, $state, $vlr, $region, $mcc, $hni, $subscription);

	# Get params from request
	$title = params->{title};
	$dates = params->{dates};
	$message = params->{message};
	($type,$state) = split ',', $event_type_to_state_hash->{params->{message_type}} if defined params->{message_type} && defined $event_type_to_state_hash->{params->{message_type}};
	$region = params->{region};
	$vlr = params->{vlr};
	$mcc = params->{mcc};
	$hni = params->{hni};
	$subscription = params->{subscription};

	# Check if all params are provided
	return api_error('MISSING_OR_INVALID_TITLE') if (!$title);
	return api_error('MISSING_OR_INVALID_DATES') if (!$dates);
	return api_error('MISSING_OR_INVALID_MESSAGE') if (!$message);
	return api_error('MISSING_OR_INVALID_MESSAGE_TYPE') if (!$type || !$state);

	my ($start_date, $end_date, $dtstart, $dtend, $now, $res);
	my ($cid, $sql_todate, $status);
	my ($sql_cols, $sql_qm, @sql_params);

	# Validate dates
	$start_date = $dates->[0];
	$end_date = $dates->[1];
	$dtstart = datetime_to_timestamp($start_date);
	$dtend = datetime_to_timestamp($end_date);
	$now = time;
	$dtstart = $now+60 if ($dtstart < $now);
	return api_error('INVALID_DATE_VALUES') if ($dtend < $dtstart);

	$cid = get_value_from_db("SELECT ".get_sql('nextval', 'SEQ_MARKETING_CAMPAIGNS', 1).get_sql('dual'));
	$sql_todate = get_sql('to_date', '?', 'YYYY-MM-DD HH24:MI:SS');
	$status = 0;

	$sql_cols = "ID, TITLE, MESSAGE_ID, CREATION_DATE, START_DATE, END_DATE";
	$sql_qm = "?, ?, ?, ".get_sql('sysdate').", $sql_todate, $sql_todate";
	@sql_params = ($cid, $title, $message, $start_date, $end_date);

	if (defined($type) && $type) {
		$sql_cols .= ", IN_OUT";
		$sql_qm .= ",?";
		push(@sql_params, $type);
	}

	if (defined($state) && $state) {
		$sql_cols .= ", VISITING_BACK";
		$sql_qm .= ",?";
		push(@sql_params, $state);
	}

	if (defined($vlr) && $vlr) {
		$sql_cols .= ", VLR";
		$sql_qm .= ",?";
		push(@sql_params, $vlr);
	}

	if (defined($region) && $region) {
		$sql_cols .= ", REGION_ID";
		$sql_qm .= ",?";
		push(@sql_params, $region);
	}

	if (defined($mcc) && $mcc) {
		$sql_cols .= ", MCC";
		$sql_qm .= ",?";
		push(@sql_params, $mcc);
	}

	if (defined($hni) && $hni) {
		$sql_cols .= ", HNI";
		$sql_qm .= ",?";
		push(@sql_params, $hni);
	}

	if (defined($subscription) && $subscription) {
		$sql_cols .= ", SUBSCRIPTION_ID";
		$sql_qm .= ",?";
		push(@sql_params, $subscription);
	}

	if (execute_sql($dbh, "INSERT INTO TBL_MARKETING_CAMPAIGNS (".$sql_cols.") VALUES (".$sql_qm.")", @sql_params) < 1) {
		return api_error('OPERATION_FAILED')
	}

	return {result => "OK", cid => $cid};
};

get '/download_marketing_file/:campaign_id' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access('/marketing_campaigns', 'V'));
	return api_error('INVALID_OR_MISSING_CAMPAIGN_ID') unless (params->{campaign_id});

	my ($date, $filename, $sql, $rows);

	$date = Invigo::get_date_str({date_separator => ''});
	$filename = "marketing_".params->{campaign_id}."_".$date.".csv";

	$sql = "SELECT MSISDN,".get_sql('date_to_char', 'DATE_SENT', 'YYYY-MM-DD HH24:MI:SS').",IN_OUT,VISITING_BACK,VLR,HOME_MCC,HOME_HNI,VISITED_MCC,VISITED_HNI,REGION_ID,SUBSCRIPTION_ID FROM TBL_MARKETING_CAMPAIGNS_DATA WHERE CAMPAIGN_ID=?";
	$rows = get_rows_from_db($dbh, $sql, params->{campaign_id});

	open(WRITE, '>', $config->{TEMP_DOWNLOAD_FOLDER}."/".$filename);
	print WRITE "MSISDN,DATE SENT,MESSAGE TYPE,VLR,HOME MCC,HOME HNI,VISITED MCC,VISITED HNI,REGION ID,SUBSCRIPTION ID\n";

	foreach my $row (@$rows) {
		print WRITE $row->[0].",".(defined($row->[1]) ? $row->[1] : "").",".(defined($row->[2]) && defined($row->[3]) ? $state_to_event_hash->{$row->[2].",".$row->[3]} : "").",".(defined($row->[4]) ? $row->[4] : "").",".(defined($row->[5]) ? $row->[5] : "").",".(defined($row->[6]) ? $row->[6] : "").",".(defined($row->[7]) ? $row->[7] : "").",".(defined($row->[8]) ? $row->[8] : "").",".(defined($row->[9]) ? $row->[9] : "").",".(defined($row->[10]) ? $row->[10] : "")."\n";
	}

	close(WRITE);

	my $file = path($config->{TEMP_DOWNLOAD_FOLDER}, $filename);
	
	return send_file($file, system_path => 1, content_disposition => 'attachement', content_type => 'text/csv', filename => $filename);
};

get '/delete_temp_file/:campaign_id' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless (has_access("ACCESS_API", 'V') || has_access('/marketing_campaigns', 'V'));
	return api_error('INVALID_OR_MISSING_CAMPAIGN_ID') unless (params->{campaign_id});

	my ($date, $filename);

	$date = Invigo::get_date_str({date_separator => ''});
	$filename = $config->{TEMP_DOWNLOAD_FOLDER}."/marketing_".params->{campaign_id}."_".$date.".csv";

	unlink($filename) if (-e $filename);
	
	return {result => "OK"};
};


=head2 /v1/monitoring/callback

This is used for 5G NEF MonitoringEvent API callbacks
See T8 reference point, TS 29.122

To achieve this, we first need to register on NEF (network exposure function)
{apiRoot}/3gpp-monitoring-event/v1/{scsAsId}/subscriptions
This is done by sending a POST request with a MonitoringEventSubscription
We will receive back HTTP status 201 with the "Location" header for the path
of the subscription

Example:
Required: notificationDestination, monitoringType
AnyOf: maximumNumberOfReports, monitorExpireTime

{
	"notificationDestination": "http://thispath/v1/monitoring/callback",
	"monitoringType": "ROAMING_STATUS",
	"monitorExpireTime": "2099-12-24T12:12:12.521Z",
	"plmnIIndication": true
}


We should receive as callback something similar to this

{
	"subscription": "http://nef.example.com/3gpp-monitoring-event/v1/myNetapp/subscriptions/{subscriptionId}",
	"monitoringEventReports": [
		{
			"monitoringType": "ROAMING_STATUS",
			"plmnId": {
				"mcc": "123",
				"mnc": "12",
			},
			"roamingStatus": true,
			"msisdn": "+96171717171"
		}
	],
}

We return back HTTP status 204 (successful notification)

Authentication/Authorization is out-of-scope but recommended to be OAuth2
or client certificates

=cut

post '/v1/monitoring/callback' => sub {
	return api_error('ACTION_NOT_ALLOWED') unless has_access("ACCESS_API", 'A');

	if (!params->{subscription} || ! params->{monitoringEventReports}) {
		Invigo::log_debug("Missing subscription or monitoringEventReports in request, skipping");
		status 400; # bad request
		return;
	}

	unless (ref (params->{monitoringEventReports}) eq 'ARRAY' ) {
		Invigo::log_debug("monitoringEventReports isn't an array, skipping");
		status 400; # bad request
		return;
	}

	Invigo::log_debug("Got reports for subscription: ". params->{subscription});
	my @reports = @{params->{monitoringEventReports}};
	for my $report (@reports) {
		Invigo::log_debug("Report: ".Dumper ($report), {level => 9});
		my $monitoringType = $report->{monitoringType};
		if (!$monitoringType || uc $monitoringType ne 'ROAMING_STATUS') {
			Invigo::log_debug("Not a ROAMING_STATUS event, continuing");
			next;
		}
		my $msisdn = $report->{msisdn}; # Identifies the MS internal PSTN/ISDN number, string formatted according to clause 3.3 of 3GPP TS 23.003 that describes an MSISDN
		my $plmnId = $report->{plmnId}; # Represents the identifier of a serving PLMN. mcc, mnc object
		my $roamingStatus = $report->{roamingStatus}; # boolean
		my $externalId = $report->{externalId}; # string containing a local identifier followed by "@" and a domain identifier. Both the local identifier and the domain identifier shall be encoded as strings that do not contain any "@" characters. See Clause 4.6.2 of 3GPP TS 23.682 for more information.
		my @externalIdParts = split /,/, $externalId;
		my $imsi = $externalIdParts[0];
		my $domain = $externalIdParts[1];
		my $eventTime = $report->{eventTime}; # string with format "date-time" as defined in OpenAPI.
		unless ($msisdn && $plmnId) {
			Invigo::log_debug("Missing msisdn, plmnId, continuing");
			next;
		}
		my ($roamer_type, $mismatch) = get_roamer_type({msisdn=>$msisdn , imsi=>$imsi });
		my $roamer = {
			event_data => $eventTime,
			imsi => $imsi,
			msisdn => $msisdn,
			type => $roamer_type,
			state => $roamingStatus ? 'V' : 'B'
		};
		$roamer->{home_hni} = get_operator_hni({imsi=>$roamer->{imsi}}) || substr($roamer->{imsi},0,5);
		$roamer->{home_mcc} = get_country_mcc({imsi=>$roamer->{imsi}}) || substr($roamer->{imsi},0,3);
		if ($roamingStatus) {
			$roamer->{vlr} = $msisdn;
			$roamer->{visited_hni} = $plmnId->{mcc}.$plmnId->{mnc};
			$roamer->{visited_mcc} = $plmnId->{mcc};
		}

		$config->{smsw_backend}->generate_trip_id($roamer);
		$config->{smsw_backend}->generate_event_id($roamer);
		# save/update status of subscriber using $config->{smsw_backend}
		$config->{smsw_backend}->add_roamer($dbh, $roamer);
	}
	status 204;
	return "";
};


hook after =>  sub {
	my ($response) = @_;
	my $username = session('username');
	Invigo::API::Utilities::log_api_calls(request , response , vars ,  $username);
};

#############
# FUNCTIONS #
#############

sub specific_action_after_select_for_rt_subscription {
	my ($rows, $columns_indexes) = @_;
	shift @$rows;
}

sub retreive_blacklist_info_hash {
	my ($arg_ref)=@_;
	my $retreive_ui_table_info= exists $arg_ref->{ui_table_cols}? $arg_ref->{ui_table_cols} : 0; delete $arg_ref->{ui_table_cols};

	my $blacklist_info_hash={
		imsi=>{tbl=>'TBL_BLACKLISTED_PREFIXES',tbl_key=>'IMSI_PREFIX',no_msg_id=>1,ui_columns=>[{ ID => "ID", HIDE => 1},{ID => "IMSI", WIDTH => 120},{ID=>"Message Type", WIDTH => 220}]},
		countries=>{tbl=>'TBL_COUNTRIES_MESSAGES',tbl_key=>'MCC',ui_columns=>[{ ID => "ID",HIDE=>1},{ ID => "MCC",  WIDTH => 100},{ ID => "Country Name", WIDTH => 200},{ID=>"Message Type"}]},
		operators=>{tbl=>'TBL_OPERATORS_MESSAGES',tbl_key=>'HNI', ui_columns=>[{ ID => "ID",HIDE=>1},{ ID => "HNI",, WIDTH => 150},{ ID => "Country Name", WIDTH => 200},{ID=>"Operator Name", WIDTH => 200},{ID=>"Message Type"}]},
		msisdn=>{tbl=>'TBL_PROVISIONED_SUBS', tbl_key=>'MSISDN', ui_columns=>[{ ID => "ID",HIDE=>1},{ ID => "MSISDN", WIDTH => 150},{ID=>"Message Type", WIDTH => 220}]}
	};

	my $result={};

	foreach (keys %{$blacklist_info_hash}){
		if ($retreive_ui_table_info) {
			$result->{$_}=$blacklist_info_hash->{$_}->{ui_columns};
		} else{
			delete $blacklist_info_hash->{$_}->{ui_columns};
			$result->{$_}=$blacklist_info_hash->{$_};
			$result->{$_}->{key} = lc $blacklist_info_hash->{$_}->{tbl_key};
		}
	}

	return $result;
}

sub pause_resume_all_campaigns {
	my ($action, $where) = @_;
	my ($old_status, $campaigns, @successful, $result, $table);

	$table = "TBL_".uc($where)."_CAMPAIGNS";
	$old_status = 0;

	$old_status = ($where eq 'bulk' ? 3 : 2) if ($action eq 'resume');
	$campaigns = get_rows_from_db("SELECT ID FROM $table WHERE STATUS=?", $old_status);
	@successful = ();
	$result = 1;

	foreach my $row (@$campaigns) {
		my ($cid, $res);

		$cid = $row->[0];
		$res = pause_resume_campaign($cid, $action, $where);
		if ($res->{result}) {
			push(@successful, $cid);
		} else {
			# Rollback action for successful ones
			my $rollback_action = ($action eq 'resume' ? 'pause' : 'resume');
			foreach my $id (@successful) {
				pause_resume_campaign($id, $rollback_action, $where);
			}
			$result = 0;
			last;
		}
	}

	return {result => $result};
}

sub pause_resume_campaign {
	my ($cid, $action, $where) = @_;
	my ($old_status, $new_status, $result, $table);

	$table = "TBL_".uc($where)."_CAMPAIGNS";

	$old_status = 0; $new_status = 0;
	$old_status = ($where eq 'bulk' ? 3 : 2) if ($action eq 'resume'); 
	$new_status = ($where eq 'bulk' ? 3 : 2) if ($action eq 'pause'); 
	$result = execute_sql($dbh,"UPDATE $table SET STATUS=? WHERE ID=? AND STATUS=?", $new_status, $cid, $old_status);

	if ($where eq 'bulk') {
		execute_sql($dbh, "UPDATE TBL_BULK_CAMPAIGNS_DATA SET STATUS=? WHERE CAMPAIGN_ID=? AND STATUS=?", $new_status, $cid, $old_status);
		execute_sql($dbh, "UPDATE TBL_SMSQUEUE_SEND SET STATUS=? WHERE ID IN (SELECT MESSAGE_ID FROM TBL_BULK_CAMPAIGNS_DATA WHERE CAMPAIGN_ID=? AND STATUS=?)", $new_status, $cid, $new_status);
	}

	return {result => $result, status => $new_status}
}

sub cancel_campaign {
	my ($cid, $where) = @_;
	my ($campaign_started, $new_status, $result, $table);

	$table = "TBL_".uc($where)."_CAMPAIGNS";

	$campaign_started = get_rows_from_db($dbh, "SELECT CASE WHEN START_DATE > ".get_sql('sysdate')." THEN '0' ELSE '1' END AS STARTED FROM $table WHERE ID=?", $cid, {arrayref_of_hashrefs => 1});
	$campaign_started = $campaign_started->[0]->{'started'};

	$new_status = ($where eq 'bulk' ? 6 : 5);
	$new_status = ($where eq 'bulk' ? 5 : 4) if ($campaign_started);
	$result = execute_sql($dbh, "UPDATE $table SET STATUS=? WHERE ID=?", $new_status, $cid);

	if ($where eq 'bulk') {
		execute_sql($dbh, "UPDATE TBL_BULK_CAMPAIGNS_DATA SET STATUS=? WHERE CAMPAIGN_ID=? AND STATUS=?", 9, $cid, 0);
		# execute_sql($dbh, "UPDATE TBL_BULK_CAMPAIGNS_DATA SET STATUS=? WHERE CAMPAIGN_ID=? AND STATUS=? AND SENDING_ID IS NOT NULL", 8, $cid, 1);
		execute_sql($dbh, "DELETE FROM TBL_SMSQUEUE_SEND WHERE ID IN (SELECT MESSAGE_ID FROM TBL_BULK_CAMPAIGNS_DATA WHERE CAMPAIGN_ID=? AND STATUS=?)", $cid, 9);
	}

	return {result => $result, status => $new_status};
}

sub get_bulk_campaign_info {
	my ($campaign_id, $with_stats) = @_;
	my ($dates_sql, $rows, $messages);

	$dates_sql = get_sql('date_to_char', 'CREATION_DATE', 'YYYY-MM-DD HH24:MI:SS')." AS CREATION_DATE, ".
			get_sql('date_to_char', 'START_DATE', 'YYYY-MM-DD HH24:MI:SS')." AS START_DATE, ".
			get_sql('date_to_char', 'END_DATE', 'YYYY-MM-DD HH24:MI:SS')." AS END_DATE";
	
	$rows = get_table_hash($dbh, 'TBL_BULK_CAMPAIGNS', {
		id => 'ID',
		cols => "ID, TITLE, STATUS, REQUIRE_REPORT,".$dates_sql.", SENDING_HOURS, FILENAME, TOTAL_COUNT, PROCESSED_COUNT, MESSAGE_ID",
		where => "ID=$campaign_id"}
	);
	return {} if (!$rows || !defined $rows->{$campaign_id});

	my $campaign = $rows->{$campaign_id};

	if ($with_stats) {
		my ($message_stats, $pending_count);
		my ($parts_stats, $delivery_reports_active);

		$message_stats = get_rows_from_db($dbh, "SELECT TOTAL_COUNT, INVALID_COUNT, NOT_QUEUED_COUNT, CANCELED_COUNT, FAILED_COUNT, SENT_COUNT FROM TBL_BULK_CAMPAIGNS WHERE ID=?", $campaign_id, {arrayref_of_hashrefs => 1});
		$pending_count = get_value_from_db("SELECT COUNT(*) FROM TBL_BULK_CAMPAIGNS_DATA WHERE CAMPAIGN_ID=? AND STATUS=?", $campaign_id, 0);
		if (defined($message_stats) && defined($message_stats->[0])) {
			$message_stats = $message_stats->[0];
			$campaign->{MESSAGE_STATS} = $message_stats;
			$campaign->{MESSAGE_STATS}->{PENDING_COUNT} = $pending_count;
		}

		$delivery_reports_active = $campaign->{REQUIRE_REPORT};
		if ($delivery_reports_active) {
			$parts_stats->{SUBMITTED_DELIVERED_COUNT} = get_value_from_db($dbh, "SELECT COUNT(*) FROM TBL_BULK_CAMPAIGNS_DATA BCD INNER JOIN TBL_BULK_MESSAGES_PARTS BMP ON (BCD.MESSAGE_ID=BMP.MESSAGE_ID) WHERE BCD.CAMPAIGN_ID=? AND BMP.STATUS=?", $campaign_id, 2);
			$parts_stats->{SUBMITTED_PENDING_COUNT} = get_value_from_db($dbh, "SELECT COUNT(*) FROM TBL_BULK_CAMPAIGNS_DATA BCD INNER JOIN TBL_BULK_MESSAGES_PARTS BMP ON (BCD.MESSAGE_ID=BMP.MESSAGE_ID) WHERE BCD.CAMPAIGN_ID=? AND BMP.STATUS IN (?,?)", $campaign_id, 1, 10);
			$parts_stats->{SUBMITTED_UNDELIVERED_COUNT} = get_value_from_db($dbh, "SELECT COUNT(*) FROM TBL_BULK_CAMPAIGNS_DATA BCD INNER JOIN TBL_BULK_MESSAGES_PARTS BMP ON (BCD.MESSAGE_ID=BMP.MESSAGE_ID) WHERE BCD.CAMPAIGN_ID=? AND BMP.STATUS=?", $campaign_id, 8);
		} else {
			$parts_stats->{SUBMITTED_SUCCESSFUL_COUNT} = get_value_from_db($dbh, "SELECT COUNT(*) FROM TBL_BULK_CAMPAIGNS_DATA BCD INNER JOIN TBL_BULK_MESSAGES_PARTS BMP ON (BCD.MESSAGE_ID=BMP.MESSAGE_ID) WHERE BCD.CAMPAIGN_ID=? AND BMP.STATUS IN (1,2,8,10)", $campaign_id);
		}

		$parts_stats->{NOT_SUBMITTED_EXPIRED_COUNT} = get_value_from_db($dbh, "SELECT COUNT(*) FROM TBL_BULK_CAMPAIGNS_DATA BCD INNER JOIN TBL_BULK_MESSAGES_PARTS BMP ON (BCD.MESSAGE_ID=BMP.MESSAGE_ID) WHERE BCD.CAMPAIGN_ID=? AND BMP.STATUS=?", $campaign_id, 7);
		$parts_stats->{NOT_SUBMITTED_PENDING_COUNT} = get_value_from_db($dbh, "SELECT COUNT(*) FROM TBL_BULK_CAMPAIGNS_DATA BCD INNER JOIN TBL_BULK_MESSAGES_PARTS BMP ON (BCD.MESSAGE_ID=BMP.MESSAGE_ID) WHERE BCD.CAMPAIGN_ID=? AND BMP.STATUS=?", $campaign_id, 0);
		$parts_stats->{SUBMITTED_FAILED_COUNT} = get_value_from_db($dbh, "SELECT COUNT(*) FROM TBL_BULK_CAMPAIGNS_DATA BCD INNER JOIN TBL_BULK_MESSAGES_PARTS BMP ON (BCD.MESSAGE_ID=BMP.MESSAGE_ID) WHERE BCD.CAMPAIGN_ID=? AND BMP.STATUS>2 AND BMP.STATUS NOT IN (6,7,8,10)", $campaign_id);
		$parts_stats->{TOTAL_COUNT} = get_value_from_db($dbh, "SELECT COUNT(*) FROM TBL_BULK_CAMPAIGNS_DATA BCD INNER JOIN TBL_BULK_MESSAGES_PARTS BMP ON (BCD.MESSAGE_ID=BMP.MESSAGE_ID) WHERE BCD.CAMPAIGN_ID=?", $campaign_id);
		$campaign->{PARTS_STATS} = $parts_stats;
	}

	return $campaign;
}

sub get_marketing_campaign_info {
	my ($campaign_id) = @_;
	my ($dates_sql, $rows, $messages);

	$dates_sql = get_sql('date_to_char', 'CREATION_DATE', 'YYYY-MM-DD HH24:MI:SS')." AS CREATION_DATE, ".
			get_sql('date_to_char', 'START_DATE', 'YYYY-MM-DD HH24:MI:SS')." AS START_DATE, ".
			get_sql('date_to_char', 'END_DATE', 'YYYY-MM-DD HH24:MI:SS')." AS END_DATE";

	$rows = get_table_hash($dbh, 'TBL_MARKETING_CAMPAIGNS', {
		id => 'ID',
		cols => "ID, TITLE, STATUS,".$dates_sql.", MESSAGE_ID, IN_OUT, VISITING_BACK, VLR, REGION_ID, MCC, HNI, SUBSCRIPTION_ID",
		where => "ID=$campaign_id"}
	);
	return {} if (!$rows || !defined $rows->{$campaign_id});

	my $campaign = $rows->{$campaign_id};

	$campaign->{MESSAGE_TYPE} = $state_to_event_hash->{$campaign->{IN_OUT}.",".$campaign->{VISITING_BACK}};
	$campaign->{REGION} = (defined($cache->{tbl_region}->{$campaign->{REGION_ID}}) && $cache->{tbl_region}->{$campaign->{REGION_ID}} ? $cache->{tbl_region}->{$campaign->{REGION_ID}} : 'Any');
	$campaign->{COUNTRY} = (defined($cache->{rt_countries}->{$campaign->{MCC}}) && $cache->{rt_countries}->{$campaign->{MCC}} ? $cache->{rt_countries}->{$campaign->{MCC}} : 'Any');
	$campaign->{OPERATOR} = (defined($cache->{rt_operators}->{$campaign->{HNI}}) && $cache->{rt_operators}->{$campaign->{HNI}} ? $cache->{rt_operators}->{$campaign->{HNI}} : 'Any');
	$campaign->{SUBSCRIPTION} = (defined($cache->{rt_subscription}->{$campaign->{SUBSCRIPTION_ID}}) && $cache->{rt_subscription}->{$campaign->{SUBSCRIPTION_ID}} ? $cache->{rt_subscription}->{$campaign->{SUBSCRIPTION_ID}} : 'Any');
	$campaign->{VLR} = 'Any' if (!$campaign->{VLR});

	$campaign->{MESSAGES_SENT} = get_value_from_db($dbh, "SELECT COUNT(MSISDN) FROM TBL_MARKETING_CAMPAIGNS_DATA WHERE CAMPAIGN_ID=?", $campaign_id);

	return $campaign;
}

sub datetime_to_timestamp{
	my ($datetime) = @_;
	my ($year, $mon, $day, $hour, $min) = split(/[\s\-:]+/, $datetime);
	return timelocal(0, $min, $hour, $day, $mon-1, $year);
}

sub create_campaign{
	my ($title, $start_date, $end_date, $require_report, $sending_hours, $message_id) = @_;
	my ($dtstart, $dtend, $now, $res);
	my ($cid, $sql_todate, $status, $filename);
	my ($user, $user_info, $user_id, $sql);

	$dtstart = datetime_to_timestamp($start_date);
	$dtend = datetime_to_timestamp($end_date);
	$now = time;
	$dtstart = $now+60 if ($dtstart < $now);
	$res = {error => '', info => ''};

	if ($dtend < $dtstart) {
		$res->{error} .= "Date end cannot be less than or equal to date start";
	}
	return $res if ($res->{error});

	$cid = get_value_from_db("SELECT ".get_sql('nextval', 'SEQ_BULK_CAMPAIGNS', 1).get_sql('dual'));
	$sql_todate = get_sql('to_date', '?', 'YYYY-MM-DD HH24:MI:SS');
	$status = 0;
	$filename = "";

	$filename = "Bulk_".$cid.".txt" ;

	$user = session('username');
	$user_info = get_user_info($user);
	$user_id = $user_info->{ID};

	$sql = "INSERT INTO TBL_BULK_CAMPAIGNS (ID, USER_ID, TITLE, MESSAGE_ID, REQUIRE_REPORT, CREATION_DATE, START_DATE, END_DATE, SENDING_HOURS, FILENAME) VALUES(?, ?, ?, ?, ?, ".get_sql('sysdate').", $sql_todate, $sql_todate, ?, ?)";
	if (execute_sql($sql, $cid, $user_id, $title, $message_id, ($require_report ? 1 : 0), $start_date, $end_date, $sending_hours, $filename) < 1) {
		$res->{error} .= "Failed to create campaign";
	} else {
		$res->{info} = "Queued for Processing";
		$res->{status} = $status;
		$res->{id} = $cid;
	}

	return $res;
}

sub add_to_mapfilter {
	my ($dbh, $roamer, $extra) = @_;
	my ($sql, $affected, @sql_params, $sql_cols, $sql_qm);

	# Initialize sql columns and params
	$sql_cols = "ID, MSISDN, VLR, IMSI, EVENTDATE, TYPE";
	$sql_qm = get_sql('nextval', 'SEQ_MAPFILTER_ID').",?,?,?,".get_sql('to_date', '?', 'YYYY-MM-DD HH24:MI:SS').",?";
	@sql_params = ($roamer->{msisdn}, $roamer->{vlr}, $roamer->{imsi}, $roamer->{event_date}, $roamer->{map_message});

	# Check if we support cell id
	if ($extra->{manage_cell_id}) {
		$sql_cols .= ", CELL_ID_STATUS";
		$sql_qm .= ",?";
		push(@sql_params, $extra->{cell_id_defined});

		if ($extra->{cell_id_defined}) {
			$sql_cols .= ", CELL_ID";
			$sql_qm .= ",?";
			push(@sql_params, $extra->{cell_id});
		}
	}

	# Add to mapfilter
	$affected = execute_sql($dbh, "INSERT INTO TBL_MAPFILTER (".$sql_cols.") VALUES (".$sql_qm.")", @sql_params);
	if (defined($affected) && $affected) {
		return "OK";
	} else {
		return "ERROR";
	}
}

sub send_on_wabbit {
	my ($dbh, $roamer, $extra) = @_;
	my ($id, @row);

	# Get new id
	$id = get_value_from_db($dbh, get_sql('get_nextval', 'SEQ_MAPFILTER_ID'));
	if ($config->{send_back_sri} && !$extra->{send_back_confirmation}){
		@row = ($id, $roamer->{type}, $roamer->{msisdn}, $roamer->{vlr}, $roamer->{imsi}, $roamer->{event_date}, $roamer->{map_message}, 1, '');
		$wabbit->send_smsw_traffic(\@row);
	} else {
		@row = ($id, $roamer->{type}, $roamer->{msisdn}, $roamer->{vlr}, $roamer->{imsi}, $roamer->{event_date}, $roamer->{map_message});
		# Check if we support cell id
		$extra->{send_back_confirmation} ? push @row, 1  : push @row,'' ;
		if ($extra->{manage_cell_id}) {
			push(@row, $extra->{cell_id_defined});
			push(@row, $extra->{cell_id}) if ($extra->{cell_id_defined});
		}
		push @row,1 if $extra->{send_back_confirmation};
		# Send to rabbitmq
		$wabbit->send_smsw_traffic(\@row);
	}

	return "OK";
}

sub get_countries_columns {

	my $foreign_language = @_;

	my $countries_table_info;
	$countries_table_info->{COLUMNS}=[
		{ID=>"ID",HIDE=>1},
		{ID=>"Flag", WIDTH => 75},
		{ID=>"MCC"},
		{ID=>"Country Name", WIDTH => 175},
		{ID=>"Neighbour coverage", WIDTH => 200},
		{ID=>"Country Codes", WIDTH => 260}
	];
	
	$countries_table_info->{COLUMNS}=[
		{ID=>"ID",HIDE=>1},
		{ID=>"Flag", WIDTH => 75},
		{ID=>"MCC", WIDTH => 80},
		{ID=>"Country Name", WIDTH => 180},
		{ID=>"Country Name in ". $config->{COUNTRIES_FOREIGN_LANGUAGE}},
		{ID=>"Neighbour coverage", WIDTH => 200},
		{ID=>"Country Codes", WIDTH => 260}
	] if $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && $config->{COUNTRIES_FOREIGN_LANGUAGE};

	$countries_table_info->{SORT_COLUMNS}={ID=>1,
				Flag=>0,
				MCC=>1, 
				'Country Name'=>1,
				'Neighbour coverage'=>1,
				'Country Codes'=>0};
	$countries_table_info->{SORT_COLUMNS}->{"Country Name in ". $config->{COUNTRIES_FOREIGN_LANGUAGE}}=true if $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && $config->{COUNTRIES_FOREIGN_LANGUAGE};
	return $countries_table_info;
}
sub get_operator_columns {

	return [
		{ID=>"HNI"},
		{ID=>"Country Name", WIDTH=>225},
		{ID=>"Operator Name", WIDTH=>250},
		{ID=>"Operator Name in ". $config->{COUNTRIES_FOREIGN_LANGUAGE}, WIDTH=>250},
		{ID=>"Preferred Operator",WIDTH=>100},
		{ID=>"Operator Prefixes",WIDTH=>175},

	]  if $config->{prefered_operators_enabled} && $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && $config->{COUNTRIES_FOREIGN_LANGUAGE};

	return [
		{ID=>"HNI"},
		{ID=>"Country Name", WIDTH=>225},
		{ID=>"Operator Name", WIDTH=>250},
		{ID=>"Preferred Operator",WIDTH=>100},
		{ID=>"Operator Prefixes",WIDTH=>175}
	]  if $config->{prefered_operators_enabled};

	return [
		{ID=>"HNI"},
		{ID=>"Country Name", WIDTH=>225},
		{ID=>"Operator Name",WIDTH=>250},
		{ID=>"Operator Name in ". $config->{COUNTRIES_FOREIGN_LANGUAGE}, WIDTH=>250},
		{ID=>"Operator Prefixes",WIDTH=>175}
	] if $config->{COUNTRIES_FOREIGN_LANGUAGE_ABBR} && $config->{COUNTRIES_FOREIGN_LANGUAGE};

	return [
		{ID=>"HNI"},
		{ID=>"Country Name", WIDTH=>225},
		{ID=>"Operator Name",WIDTH=>250},
		{ID=>"Operator Prefixes",WIDTH=>175}
	];

}

sub _get_documentation_array {
	my ($base_url) = @_;

	return [
		{title => 'Introduction'},
		{
			title => 'API usage', html_id => 'usage',
			text => ["Invigo's SmsWelcome platform offers rich and flexible HTTP APIs to interface with operator's server nodes.",
				"The APIs include a set of HTTP functions to read roamer data, history and other information in addition to performing actions on the platform like blacklisting and sending messages.",
				"This document describes the set of HTTP functions part of the SmsWelcome HTTP APIs.",
				"The API base URL will depend on every installation, and must be accessed over https. For this document the following base URL will be used as an example: $base_url",
				"The HTTP APIs functions are RESTful and return values are in JSON format. They should be parsed using a JSON parser. This means spaces and tabs between objects and values must be ignored and the order of properties inside objects can change.",
				"The following two examples represnt the same object.",
				qq[{"foo":1,"bar":2}] ,	"and", "{" , qq[ "bar" : 2 ,] , qq[	"foo" : 1 ] , "}" ,
				"Whenever any post DATA needs to be sent, it should be encoded as a JSON object and a request header 'Content-Type' should be set to 'application/json'",
				"Whenever an API call succeeds, an HTTP response code 200 is returned along with the return object.",
				"For the calls that perform an action, a result {\"status\" : \"OK\"} is returned in the response content, this response can contain additional info when sepcified in the documentation.",
				"All API calls can return errors in case something goes wrong. The possible error IDs are listed in Appendix A and have the following form",
				"{ \"error\" : { \"id\" : \"ERROR_ID_STRING\", \"old_code\" : 0 } }",
				"",
				"",
				"",
			]
		},
		{title => 'Authentication'},
		{
			title => 'General',
			text => ["The API uses basic HTTP authentication as described here",
				"https://en.wikipedia.org/wiki/Basic_access_authentication",
				"The user of the API must provide a basic Authorization header with every request as such",
				"Authorization: Basic QWxhZGRpbjpPcGVuU2VzYW1l",
				"The Authorization field is constructed as follows:",
				"- The username and password are combined with a single colon.",
				"- The resulting string is encoded into an octet sequence.",
				"- The resulting string is encoded using a variant of Base64.",
				"- The authorization method and a space i.e. \"Basic \" is then put before the encoded string.",
				"",
				"Alternatively when testing from a browser the user can prepend username:password@ to the hostname",
				"example: ". do {my $u = $base_url; $u=~s{https://}{https://username:password@}; $u},
				"",
				"In the following examples the autentication is omitted even though *Authorization headers must be sent with every request*",
			]
		},
		{
			id => '/authentication/login', methods => [qw(post get)], pod => 'get /authentication/login',
			title => 'Log in',
			examples => [
				['/authentication/login', 'Correct username/password combination', {username=>'admin', password=>'correct'}, '{"status":"OK"}'],
				['/authentication/login', 'Wrong username/password combination', {username=>'admin', password=>'wrong'}, '{"error":{"id":"NOT_AUTHENTICATED","old_code":101}}']
			]
		},
		{
			id => '/authentication/logout', methods => [qw(post get)], 
			title => 'Log out',
			examples => [
				['/authentication/logout', undef, undef, '{"status":"OK"}']
			]
		},
		{title => 'Roamer'},
		{
			id => '/get_roamer_info', methods => [qw(get)],
			title => 'Get a roamer\'s info from DB',
			examples => [
				['/get_roamer_info', 'With MSISDN provided', {msisdn => 971561557174}, '{"result":"OK","info":{"TYPE":"O",MSISDN":"971561557174","IMSI":"424021473189324","DATE_IN":"2020-07-01 13:21","DATE_LAST":"2020-07-01 15:17","DATE_OUT":null,"HOME_COUNTRY":424,"HOME_COUNTRY_NAME":"United Arab Emirates","HOME_OPERATOR":42402,"HOME_OPERATOR_NAME":"Etisalat","VIS_COUNTRY":415,"VIS_COUNTRY_NAME":"Lebanon","VIS_OPERATOR":41501,"VIS_OPERATOR_NAME":"alfa","VLR_LOCATION":"9613499728","NUMBER_SMS_SENT":1}}'],
				['/get_roamer_info', 'With IMSI provided', {imsi => 424021473189324}, '{"result":"OK","info":{"TYPE":"O",MSISDN":"971561557174","IMSI":"424021473189324","DATE_IN":"2020-07-01 13:21","DATE_LAST":"2020-07-01 15:17","DATE_OUT":null,"HOME_COUNTRY":424,"HOME_COUNTRY_NAME":"United Arab Emirates","HOME_OPERATOR":42402,"HOME_OPERATOR_NAME":"Etisalat","VIS_COUNTRY":415,"VIS_COUNTRY_NAME":"Lebanon","VIS_OPERATOR":41501,"VIS_OPERATOR_NAME":"alfa","VLR_LOCATION":"9613499728","NUMBER_SMS_SENT":1}}'],
				['/get_roamer_info', 'With no roamer identification provided', {}, '{"error":{"id":"MISSING_ROAMER_IDENTIFICATION","old_code":null}}']
			]
		},
		{
			id => '/get_roamer_history', methods => [qw(get)],
			title => 'Get a roamer\'s history from DB',
			examples => [
				['/get_roamer_history', 'With MSISDN provided', {msisdn => 971561557174}, '{"result":"OK","history":{"0":{"EVENT":"Start Trip",IN_OUT":"O","VISITING_BACK":"V","MSISDN":"971561557174","IMSI":"424021473189324","DATE_IN":"2020-07-01 13:21","DATE_LAST":"2020-07-01 15:17","DATE_OUT":null,"HOME_COUNTRY":424,"HOME_OPERATOR":42402,"VIS_COUNTRY":415,"VIS_OPERATOR":41501,"VLR_LOCATION":"9613499728","NUMBER_SMS_SENT":null},"1":{"EVENT":"Changed Country",IN_OUT":"O","VISITING_BACK":"V","MSISDN":"971561557174","IMSI":"424021473189324","DATE_IN":"2020-07-01 13:21","DATE_LAST":"2020-07-02 10:53","DATE_OUT":null,"HOME_COUNTRY":424,"HOME_OPERATOR":42402,"VIS_COUNTRY":208,"VIS_OPERATOR":20801,"VLR_LOCATION":"33159472536","NUMBER_SMS_SENT":2}}}'],
				['/get_roamer_history', 'With no roamer identification provided', {}, '{"error":{"id":"MISSING_ROAMER_IDENTIFICATION","old_code":null}}']
			]
		},
		{
			id => '/get_roamer_stats', methods => [qw(get)],
			title => 'Get a roamer\'s stats from DB',
			examples => [
				['/get_roamer_stats', 'With MSISDN provided', {msisdn => 971561557174}, '{"result":"OK","stats":{"TYPE":"O",MSISDN":"971561557174","IMSI":"424021473189324","DATE_LAST_IN":"2020-07-01 13:21","DATE_LAST_OUT":null,"DURATION_LAST_TRIP":3,"TOTAL_TRIPS_DAYS":10,"TOTAL_NUMBER_OF_TRIPS":4,"AVERAGE_DURATION":2,"NUM_WSMS_SENT":0,"NUM_WMMS_SENT":0,"NUM_WINBACK_SENT":0,"NUM_BONVOYAGE_SENT":4,"NUM_LOCALSMS_SENT":0,"NUM_BACK_SENT":2}}'],
				['/get_roamer_stats', 'With no roamer identification provided', {}, '{"error":{"id":"MISSING_ROAMER_IDENTIFICATION","old_code":null}}']
			]
		},
		{
			id => '/blacklist/:type', methods => [qw(post put)],
			title => 'Blacklist or unblacklist a value of a certain type: Roamer, Country, or Operator',
			examples => [
				['/blacklist/roamer', 'Blacklisting a roamer with MSISDN provided', {msisdn => 971561557174, roaming_type => 'outbound'}, '{"result":"OK","type":"roamer/msisdn","direction":"in","value":"971561557174"}'],
				['/blacklist/roamer', 'Blacklisting a roamer with IMSI provided', {imsi => 424021473189324}, '{"result":"OK","type":"roamer/imsi","direction":"in","value":"424021473189324"}'],
				['/blacklist/roamer', 'Remove a roamer from blacklist with MSISDN provided', {msisdn => 971561557174, direction => 'out', roaming_state => 'visiting'}, '{"result":"OK","type":"roamer/msisdn","direction":"out","value":"971561557174"}'],
				['/blacklist/country', 'Blacklisting a country with MCC provided', {mcc => 415}, '{"result":"OK","type":"country","direction":"in","value":"415"}'],
				['/blacklist/operator', 'Blacklisting an operator with HNI provided', {hni => 41501}, '{"result":"OK","type":"operator","direction":"in","value":"41501"}'],
				['/blacklist/roamer', 'Blacklisting a roamer with wrong input parameters', {msisdn => 971561557174, roaming_type => 'wrong'}, '{"error":{"id":"INVALID_ROAMING_TYPE_FIELD","old_code":null}}'],
				['/blacklist/roamer', 'Blacklisting a roamer with a specific message type', {msisdn => 971561557174, message_type => 'embassy'}, '{"result":"OK","type":"roamer/msisdn","direction":"in","value":"971561557174"}']
			] 
		},
		{
			id => '/update_subscriber_info/:msisdn', methods => [qw(post put)],
			title => 'Add or update a subscriber\'s info: Subscription, Language, Name, Nationality...',
			examples => [
				['/update_subscriber_info/971561557174?subscription=1&language=en&name=John Doe', 'Adding/updating a subscriber with MSISDN and info provided', {msisdn => 971561557174, subscription => 1, language => 'en', name => 'John Doe'}, '{"result":"OK"}'],
				['/update_subscriber_info/971561557174?language=ab', 'Adding/updating as subscriber with invalid language', {msisdn => 971561557174, language => 'ab'}, '{"error":{"id":"INVALID_LANGUAGE_ID","old_code":null}}']
			]
		},
		{title => 'Cell IDs'},
		{
			id => '/cell_ids/groups', methods => [qw(get)],
			title => 'List all Cell ID groups in DB',
			examples => [
				['/cell_ids/groups', '', {}, '{"result":"OK","data":{"0":{"ID":1,"NAME":"Cell Group 1"},"1":{"ID":2,"NAME":"Cell Group 2"}}}']
			]
		},
		{
			id => '/cell_ids/groups/new', methods => [qw(post put)],
			title => 'Add a new Cell ID group to DB',
			examples => [
				['/cell_ids/groups/new', 'Add a new group', {name => 'New Cell Group'}, '{"result":"OK","ID":5}'],
				['/cell_ids/groups/new', 'Add a new group with existing ID', {id => 2, name => 'New Cell Group'}, '{"error":{"id":"GROUP_ID_ALREADY_IN_USE","old_code":null}}']
			]
		},
		{
			id => '/cell_ids/add', methods => [qw(post put)],
			title => 'Add a Cell ID to a group',
			examples => [
				['/cell_ids/add', 'Add a cell ID to an existing group', {group_id => 2, cell_id => 'f16tr'}, '{"result":"OK"}'],
				['/cell_ids/add', 'Add a cell ID to a non-existing group', {group_id => 6, cell_id => 'f16tr'}, '{"error":{"id":"GROUP_ID_NOT_FOUND","old_code":null}}'],
				['/cell_ids/add', 'Add an invalid cell ID', {group_id => 2, cell_id => '1'}, '{"error":{"id":"INVALID_OR_MISSING_CELL_ID_FIELD","old_code":null}}']
			] 
		},
		{title => 'Messages'},
		{
			id => '/send_message_to_roamer/:msisdn/:message_id', methods => [qw(post put)],
			title => 'Send specific message ID to roamer',
			examples => [
				['/send_message_to_roamer/971561557174/4', 'With message ID', {}, '{"result":"OK"}'],
				['/send_message_to_roamer/971561557174', 'Without message ID', {}, '{"error":{"id":"MISSING_MESSAGE_ID","old_code":null}}'],
				['/send_message_to_roamer/971561557174/4', 'With extra parameters', {lang => 'en', src_addr => '1234'}, '{"result":"OK"}']
			]
		},
		{
			id => '/resend_message/:msisdn', methods => [qw(post put)],
			title => 'Resending the welcome message to a roamer',
			examples => [
				['/resend_message/971561557174', 'Resending to a roamer currently in Lebanon', {}, '{"result":"OK"}'],
				['/resend_message/971561557174', 'Resending to a roamer not currently roaming', {}, '{"error":{"id":"ROAMER_NOT_DETECTED","old_code":null}}'],
			]
		},
		{
			id => '/get_messages_for_roamer/:msisdn', methods => [qw(get)],
			title => 'Get the messages to send to roamer based on location',
			examples => [
				['/get_messages_for_roamer/971561557174/2', 'With request ID', {}, '{"messages":{"en":"This is an English test message","ar":"هذه رسالة باللغة العربية"}}'],
				['/get_messages_for_roamer/971561557174', 'Only tariff message and English language', {message_type => 'tariff', language => 'en'}, '{"messages":{"en":"This is an English test tariff message"}'],
				['/get_messages_for_roamer/971561557174/2', 'Roamer no longer roaming', {}, '{"error":{"id":"ROAMER_NOT_ROAMING","old_code":null}}']
			]
		}
	];
}

true;
