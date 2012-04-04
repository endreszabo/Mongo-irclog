use strict;
use vars qw($VERSION %IRSSI);
use Irssi;

$VERSION = '0.01';
%IRSSI = (
	name        => 'mongolog',
	authors     => 'Endre Szabo',
	contact     => 'irssi@end.re',
	description => 'This script logs/stores events to MondoDB.',
	license     => 'GPL',
);

use Time::HiRes qw/gettimeofday/;
use Storable qw/nfreeze thaw/;
use MongoDB;
use MongoDB::OID;

my $conn;
my $db;
my $coll;

my %fields=(
	'message own_private'		=> [qw/server_rec msg target orig_target/],
	'message public'		=> [qw/server_rec msg nick address target/],
	'message private'		=> [qw/server_rec msg nick address/],
	'message own_public'		=> [qw/server_rec msg target/],
	'message join'			=> [qw/server_rec channel nick address/],
	'message part'			=> [qw/server_rec channel nick address reason/],
	'message quit'			=> [qw/server_rec nick address reason/],
	'message kick'			=> [qw/server_rec channel nick kicker address reason/],
	'message nick'			=> [qw/server_rec newnick oldnick address/],
	'message own_nick'		=> [qw/server_rec newnick oldnick address/],
	'message invite'		=> [qw/server_rec channel nick address/],
	'message topic'			=> [qw/server_rec channel topic nick address/],
	'message irc action'		=> [qw/server_rec msg nick address target/],
	'message irc own_notice'	=> [qw/server_rec msg target/],
	'message irc notice'		=> [qw/server_rec msg nick address target/],
	'message irc own_ctcp'		=> [qw/server_rec cmd data target/],
	'message irc ctcp'		=> [qw/server_rec cmd data nick address target/],
	'message irc mode'		=> [qw/server_rec channel nick address mode/],
	'user mode changed'		=> [qw/server_rec old/],
	'channel mode changed'		=> [qw/channel_rec setby/],
	'query created'			=> [qw/query_rec automatic/],
	'query destroyed'		=> [qw/query_rec/],
	'query nick changed'		=> [qw/query_rec orignick/],
	'query address changed'		=> [qw/query_rec/],
	'channel created'		=> [qw/channel_rec automatic/],
	'channel destroyed'		=> [qw/channel_rec/],
	'nicklist new'			=> [qw/channel_rec nick_rec/],
	'nicklist remove'		=> [qw/channel_rec nick_rec/],
	'nicklist changed'		=> [qw/channel_rec nick_rec old_nick/],
	'nicklist host changed'		=> [qw/channel_rec nick_rec/],
	'nicklist gone changed'		=> [qw/channel_rec nick_rec/],
	'nicklist serverop changed'	=> [qw/channel_rec nick_rec/],
);

sub freeze_to_file($) {
	my $debug=Irssi::settings_get_bool('mongolog_debug');
	my $temp=Irssi::settings_get_str('mongolog_tmp_file');
	open(FD, '>>'.$temp) || die "Could not open tmp file '$temp' for dumping.";
	Irssi::print('No DB connection, storing to temp file.') if $debug;
	print FD unpack('H*',nfreeze(shift))."\n"; # is there a bettey way?
	close FD;
}

sub store_log(@) {
	my $debug=Irssi::settings_get_bool('mongolog_debug');
	my %log; my $id;
	$log{time}=join('.',gettimeofday());
	$log{signal}=Irssi::signal_get_emitted();
	Irssi::print("'$log{signal}' => '".join("', '",@_)."'.") if $debug;
	@log{@{$fields{$log{signal}}}}=@_;
	if(defined($log{server_rec})) {
		$log{server}=$log{server_rec}->{tag};
		delete($log{server_rec});
	}
	if(defined($log{channel_rec})) {
		$log{channel}=$log{channel_rec}->{name};
		delete($log{channel_rec});
	}
	if(defined($log{nick_rec})) {
		$log{address}=$log{nick_rec}->{host};
		$log{nick}=$log{nick_rec}->{nick};
		delete($log{nick_rec});
	}
	if($conn and $db and $coll and $id=$coll->insert(\%log)) {
		Irssi::print("Stored log into db with _id '$id'.") if $debug;
	} else {
		freeze_to_file(\%log);
	}
}

sub mongodb_connect{
	my $debug=Irssi::settings_get_bool('mongolog_debug');
	my $temp=Irssi::settings_get_str('mongolog_tmp_file');
	eval {
		$conn=MongoDB::Connection->new(
			host		=> Irssi::settings_get_str('mongolog_db_host'),
			query_timeout	=> Irssi::settings_get_int('mongolog_db_query_timeout'),
		);
	}; warn $@ if $@;
	return 0 if !$conn;
	$conn->authenticate($dbname, Irssi::settings_get_str('mongolog_db_username'), Irssi::settings_get_str('mongolog_db_password'));
	$db=Irssi::settings_get_str('mongolog_db_name');
	$db=$conn->$db;
	$coll=Irssi::settings_get_str('mongolog_db_collection');
	$coll=$db->$coll;
	if (-e $temp) {
		my $id;
		my %log;
		if (open(FD, '<'.$temp)) {
			while(<FD>) {
				$id=$coll->insert(thaw(pack('H*',$_)));
				Irssi::print("temp file entry iserted into DB as id '$id'.") if $debug;
			}
			close FD;
			unlink $temp;
		} else {
			die "Could not open existing tmp file '$temp' for processing.";
		}
	}
	Irssi::print('Connected to mongodb at '.Irssi::settings_get_str('mongolog_db_host'));
	return 1;
}

foreach my $signal (keys %fields) {
	Irssi::signal_add_first($signal, 'store_log')
};

Irssi::settings_add_bool('mongolog', 'mongolog_debug', 0);
Irssi::settings_add_str('mongolog', 'mongolog_db_host', 'mongodb://localhost:27017');
Irssi::settings_add_str('mongolog', 'mongolog_db_username', '');
Irssi::settings_add_str('mongolog', 'mongolog_db_password', '');
Irssi::settings_add_str('mongolog', 'mongolog_db_name', 'irclog');
Irssi::settings_add_str('mongolog', 'mongolog_db_collection', 'logs');
Irssi::settings_add_int('mongolog', 'mongolog_db_query_timeout', 2000);
Irssi::settings_add_str('mongolog', 'mongolog_tmp_file', 'mongolog.tmp');
Irssi::command_bind('mongolog_db_connect','mongodb_connect');

Irssi::print("mongolog $VERSION started");
mongodb_connect();

1;
