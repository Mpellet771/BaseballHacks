#! /usr/bin/perl 

# loads a database with this season's box scores up through yesterday's games

use save_to_db;

use DBI;
my $DSN = "dbi:mysql:database=boxes;host=localhost;user=root userID;password=passsword";
my $dbh = DBI->connect($DSN);
use XML::Simple;
my $xs = new XML::Simple(ForceArray => 1, KeepRoot => 1, KeyAttr => 'boxscore');
my $xsp = new XML::Simple(ForceArray => 1, KeepRoot => 1, KeyAttr => 'player');
use LWP;
my $browser = LWP::UserAgent->new; 

# to prevent partial loading of games in progress, only load a day's games
# after 12:00 PM GMT
use Time::Local;
my $mintime = timegm(0,0,0,12,6,115);
my $maxtime = time() - 60*60*36;
my $mintimestr = gmtime(timegm(0,0,0,1,3,116));
my $maxtimestr = gmtime(time() - 60*60*36);

print "iterating from $mintimestr to $maxtimestr\n";
for ($i = $mintime; $i <= $maxtime; $i += 86400) {
    ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = gmtime($i);
    $fmon = length($mon + 1) == 1 ? '0' . ($mon + 1) : ($mon + 1);
    $fday = length($mday) == 1 ?  '0' . $mday : $mday;

    # build base url for fetching box scores and start fetching
    $baseurl = 'http://gd2.mlb.com/components/game/mlb/year_' . (1900 + $year) .
	'/month_' . $fmon .
	'/day_' . $fday . '/';
    
    my $response = $browser->get($baseurl);
    die "Couldn't get $baseurl: ", $response->status_line, "\n"
	unless $response->is_success;
    my $dirhtml = $response->content;
    
    while($dirhtml =~ m/<a href=\"(gid_.+)\/\"/g ) {
	my $game = $1;
	print "fetching box score for game $game\n";
	my $boxurl = $baseurl . $game . "/boxscore.xml";
	my $response = $browser->get($boxurl);
	# die "Couldn't get $boxurl: ", $response->status_line, "\n"
	unless ($response->is_success) {
	    print "Couldn't get $boxurl: ", $response->status_line, "\n";
	    next;
	}
	my $box = $xs->XMLin($response->content);
	save_batting_and_fielding($dbh, $box);
	save_pitching($dbh, $box);
	save_game($dbh, $box);
	
	my $playersurl = $baseurl . $game . "/players.xml";
	my $response = $browser->get($playersurl);
	unless ($response->is_success) {
	    print "Couldn't get $playersurl: ", $response->status_line, "\n";
	    next;
	}
	my $box = $xsp->XMLin($response->content);
	save_roster($dbh, $box);
	#die;
    }
    # be a good spider and don't take up too much bandwidth
    sleep(1);
}
