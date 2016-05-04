package save_to_db;
require Exporter;
@ISA = qw(Exporter);
@EXPORT = qw (save_batting_and_fielding save_pitching save_game save_roster);

sub extract_date($) {
    my($in) = @_;
    my $gmyr = substr($in,0,4);
    my $gmmn = substr($in,5,2);
    my $gmdy = substr($in,8,2);
    my $gamedate = '\'' . $gmyr . '-' . $gmmn . '-' . $gmdy . '\'';
    return $gamedate;
}

sub extract_info($) {
    my ($box) = @_;
    my $home = $box->{boxscore}->[0]->{home_team_code};
    my $away = $box->{boxscore}->[0]->{away_team_code};
    my $gameid = "'" . $box->{boxscore}->[0]->{game_id} . "'";
    my $gamedate = extract_date($box->{boxscore}->[0]->{game_id});
    my $status_ind = $box->{boxscore}->[0]->{status_ind};    
    my $away_wins = $box->{boxscore}->[0]->{away_wins};
    my $away_loss = $box->{boxscore}->[0]->{away_loss};
    my $home_wins = $box->{boxscore}->[0]->{home_wins};
    my $home_loss = $box->{boxscore}->[0]->{home_loss}; 
    my $venue_name = $box->{boxscore}->[0]->{venue_name};   
    return ($home, $away, $gameid, $gamedate,$status_ind,$away_wins,$away_loss,$home_wins, $home_loss, $venue_name);
}

sub save_pitching($$) {
    my ($dbh, $box) = @_;
    my ($home, $away, $gameid, $gamedate, $status_ind) = extract_info($box);
    foreach $team (@{$box->{boxscore}->[0]->{pitching}}) {
	    my $count = 0;
	foreach $pitcher (@{$team->{pitcher}}) {
	    $count++; 	
	    $win=0; $loss=0; $hold=0; $holdopp=0; $save=0; $saveopp=0; $cg=0; $sho=0;
	    if ($pitcher->{out} == 27){
	    	    $cg = 1;
	    	}
	    if (($pitcher->{out} == 27) && ($pitcher->{r} == 0)){
			    $sho = 1;
		}
	    if ($count == 1){
			$gs = 1;
		}
	    else
		{
			$gs = 0;
		}
	    if      ($pitcher->{note} =~ /\(W/) { $win = 1; }
	    elsif ($pitcher->{note} =~ /\(L/)  {$loss = 1; }
	    elsif ($pitcher->{note} =~ /\(S/)  {$save = 1; $saveopp = 1;}
	    elsif ($pitcher->{note} =~ /\(BS/) {$saveopp = 1;}
	    elsif ($pitcher->{note} =~ /\(H/)  {$hold = 1; $holdopp = 1;}
	    elsif ($pitcher->{note} =~ /\(BH/) {$holdopp = 1;}
	    $ptchr_query = 'INSERT INTO pitching VALUES ('
		. join(',', (
			   $pitcher->{id},
			 "'" . ($team->{team_flag} == "away" ? 
				  $home : $away) . "'",
			   $gameid,
			   $gamedate,
			   $pitcher->{out},
			   $pitcher->{bf},
			   $pitcher->{hr},
			   $pitcher->{bb},
			   $pitcher->{so},
			   $pitcher->{er},
			   $pitcher->{r},
			   $pitcher->{h},
			   "'" . $pitcher->{era} . "'",
			   $win,
			   $loss,
			   $hold,
			   $holdopp,
			   $save,
			   $saveopp,
			   #$cg,
			   #$sho,
			   #$gs,
			   "'" . $pitcher->{s_ip} . "'",
			   )) . ")";
	    		
	       		if (($status_ind eq "F")||($status_ind eq "FR")){
				       
		    		my $sth = $dbh->prepare($ptchr_query);
				die ("MySQL Error: $dbh->$errmsg\n") unless defined($sth);
				$sth->execute() or die ("MySQL Error: $DBI::errstr\n$ptchr_query\n");
				$sth->finish(); 

			}
			else
			{

				print "status_ind: $status_ind\n";

			}
	    

	}
    }
}

sub extractvars($$) {
    # a procedure for extracting information from
    # the text data field (like stolen bases, errors, etc.)
    my ($type, $text) = @_;
    my $stuff = {};

    
    if ($text =~ m{ <b> $type <\/b>\:\s* (.*) \.<br\/> }x) {
	my @players = split /\),/, $1;
        foreach $player (@players) {
            # important: player names may include commas, spaces,
            #       apostrophes, and periods, but no numbers
            #       or parentheses
	    $player =~ /([\w\s\,\.\']+)\s(\d?)\s?\(.*/;
	    $name = $1;
	 
	    if ($2) {$num = $2;} else {$num = 1;}
	    $stuff->{$name} = $num;
	}
	
    }
    return $stuff;
     
}




sub save_batting_and_fielding($$) {
    my ($dbh, $box) = @_;    
    my ($home, $away, $gameid, $gamedate, $status_ind) = extract_info($box);
    foreach $team (@{$box->{boxscore}->[0]->{batting}}) {
	my %steals         = %{extractvars('SB', $team->{text_data}->[0])};
	my %caughtstealing = %{extractvars('CS', $team->{text_data}->[0])};
	my %errors         = %{extractvars('E',  $team->{text_data}->[0])};
	my %passedballs    = %{extractvars('PB', $team->{text_data}->[0])};

	foreach $batter (@{$team->{batter}}) {
	    my $sb = $steals{$batter->{name}};
	    $sb = 0 unless defined($sb);
	    my $cs = $caughtstealing{$batter->{name}};
	    $cs = 0 unless defined($cs);
	    my $e = $errors{$batter->{name}};
	    $e = 0 unless defined($e);
	    my $pb = $passedballs{$batter->{name}};
	    $pb = 0 unless defined($pb);

	    $batter->{pos} =  substr $batter->{pos}, 0,1;
	    
	    $batr_query = 'INSERT INTO batting VALUES ('
			. join(',', (
			   $batter->{id},
			 "'" . ($team->{team_flag}=="away" ? 
				 	$home : $away) . "'",
			   $gameid,
			   $gamedate,
			   $batter->{h},
			   $batter->{hr},
			   $batter->{bb},
			   $batter->{so},
			   $batter->{rbi},
			   $batter->{ab},
			   $batter->{r},
			   $batter->{t},
			   $batter->{d},
			   $batter->{lob},
			   $sb,
			   $cs,
			   $batter->{sf},
			   $batter->{hbp},
			   "'" . $batter->{avg} . "'"
		       )) . ")";
		      
		 
			 if (($status_ind eq "F")||($status_ind eq "FR")){
		    		my $sth = $dbh->prepare($batr_query);
				die ("MySQL Error: $dbh->$errmsg\n") unless defined($sth);
				$sth->execute() or die ("MySQL Error: $DBI::errstr\n$batr_query\n");
				$sth->finish();    
			}
			else
			{
				print "status_ind: $status_ind\n";
			}
	    
	    $fldr_query = 'INSERT INTO fielding VALUES ('
		. join(',', (
			   $batter->{id},
			   "'" . ($team->{team_flag}=="away" ? 
				  $home : $away) ."'",
			   $gameid,
			   $gamedate,
			   "'" . $batter->{pos} . "'",
			   $batter->{po},
			   $batter->{po},
			   $e,
			   $pb
		       )) . ")";
	    my $sth = $dbh->prepare($fldr_query);
			die ("MySQL Error: $dbh->$errmsg\n") unless defined($sth);
			$sth->execute() or die ("MySQL Error: $DBI::errstr\n$fldr_query\n");
			$sth->finish();	    
	}
    }
}

sub save_batting_and_fielding_away($$) {
    my ($dbh, $box) = @_;    
    my ($home, $away, $gameid, $gamedate, $status_ind) = extract_info($box);
    foreach $team (@{$box->{boxscore}->[0]->{batting}}) {
	my %steals         = %{extractvars('SB', $team->{text_data}->[0])};
	my %caughtstealing = %{extractvars('CS', $team->{text_data}->[0])};
	my %errors         = %{extractvars('E',  $team->{text_data}->[0])};
	my %passedballs    = %{extractvars('PB', $team->{text_data}->[0])};

	foreach $batter (@{$team->{batter}}) {
	    my $sb = $steals{$batter->{name}};
	    $sb = 0 unless defined($sb);
	    my $cs = $caughtstealing{$batter->{name}};
	    $cs = 0 unless defined($cs);
	    my $e = $errors{$batter->{name}};
	    $e = 0 unless defined($e);
	    my $pb = $passedballs{$batter->{name}};
	    $pb = 0 unless defined($pb);

	    $batter->{pos} =  substr $batter->{pos}, 0,1;
	    
	    $batr_query = 'INSERT INTO batting VALUES ('
			. join(',', (
			   $batter->{id},
			 "'" . ($team->{team_flag}=="away" ? 
				 	$home : $away) . "'",
			   $gameid,
			   $gamedate,
			   $batter->{h},
			   $batter->{hr},
			   $batter->{bb},
			   $batter->{so},
			   $batter->{rbi},
			   $batter->{ab},
			   $batter->{r},
			   $batter->{t},
			   $batter->{d},
			   $batter->{lob},
			   $sb,
			   $cs,
			   $batter->{sf},
			   $batter->{hbp},
			   "'" . $batter->{avg} . "'"
		       )) . ")";
		      
		 
			 if (($status_ind eq "F")||($status_ind eq "FR")){
		    		my $sth = $dbh->prepare($batr_query);
				die ("MySQL Error: $dbh->$errmsg\n") unless defined($sth);
				$sth->execute() or die ("MySQL Error: $DBI::errstr\n$batr_query\n");
				$sth->finish();    
			}
			else
			{
				print "status_ind: $status_ind\n";
			}
	    
	    $fldr_query = 'INSERT INTO fielding VALUES ('
		. join(',', (
			   $batter->{id},
			   "'" . ($team->{team_flag}=="away" ? 
				  $home : $away) ."'",
			   $gameid,
			   $gamedate,
			   "'" . $batter->{pos} . "'",
			   $batter->{po},
			   $batter->{po},
			   $e,
			   $pb
		       )) . ")";
	    my $sth = $dbh->prepare($fldr_query);
			die ("MySQL Error: $dbh->$errmsg\n") unless defined($sth);
			$sth->execute() or die ("MySQL Error: $DBI::errstr\n$fldr_query\n");
			$sth->finish();	    
	}
    }
}



sub save_game($$) {
    my ($dbh, $box) = @_;
    my ($home, $away, $gameid, $gamedate,$status_ind,$away_wins,$away_loss,$home_wins,$home_loss, $venue_name) = extract_info($box);
    $gamedate2 = $gamedate;
    $gameid2 = $gameid;
    #foreach $team (@{$box->{boxscore}->[0]->{pitching}}) {
	$game_query = 'INSERT INTO games VALUES ('
	    . join(',', (
		       $gameid,
		       $gamedate,
		       "'" . $home . "'",
		       "'" . $away . "'",
		       $away_wins,
		       $away_loss,
		       $home_wins,
		       $home_loss,
		       "'".$status_ind."'",
	  	       $home_wins."+".$home_loss,		 				       $away_wins."+".$away_loss,	
	  	     "'" .$venue_name . "'"
		   )) . ")";
	    my $sth = $dbh->prepare($game_query);
			die ("MySQL Error: $dbh->$errmsg\n") unless defined($sth);
			$sth->execute() or die ("MySQL Error: $DBI::errstr\n$game_query\n");
			$sth->finish();		    
			#}
}

sub save_roster($$)
{
	my ($dbh, $box, $roster) = @_;
	my ($home, $away, $gameid, $gamedate) = extract_info($box);

	#print $box->{game}->[0]->{team}->[0]->{player}->[0]->{last}; 
	foreach $team (@{$box->{game}->[0]->{team}}) {
		foreach $player (@{$team->{player}}) {

			$id = $player->{id};
			$first = $dbh->quote($player->{first});
			$last = $dbh->quote($player->{last});
			$throws = $dbh->quote($player->{rl});
			
			$no_duplicate_query = 'SELECT eliasID FROM rosters WHERE eliasID = ' . $id;
		    	my $sth = $dbh->prepare($no_duplicate_query);
			die ("MySQL Error: $dbh->$errmsg\n") unless defined($sth);
			$sth->execute() or die ("MySQL Error: $DBI::errstr\n$no_duplicate_query\n");
			$sth->finish();	

			my $numRows = $sth->rows;
			
			#if ($numRows) {
				# don't insert duplicate player entry into players table
				
				#} else {				

			#$roster_query = 'INSERT INTO rosters_hacks(eliasID,nameFirst, nameLast, throws)'
			#. 'VALUES (' . $id . ', '. $first . ', ' . $last . ', ' . $throws . ')';

			#my $sth = $dbh->prepare($roster_query);
			#die ("MySQL Error: $dbh->$errmsg\n") unless defined($sth);
			#$sth->execute() or die ("MySQL Error: $DBI::errstr\n$roster_query\n");
			#$sth->finish();
			#$sth->execute() or die ("MySQL Error: $DBI::errstr\n$roster_query\n");
			#$sth->finish();
			#}
			
		}
	}		
	
	foreach $team (@{$box->{game}->[0]->{team}})
	{
		foreach $player (@{$team->{player}})
		{
			$jersey = $player->{num};
			$roster_query = 'INSERT INTO rosters VALUES (' 
				.	join(',', (
					$gameid2,
					$gamedate2,
					"'" . $team->{id} . "'",
					$player->{id},
					'"' . $player->{first} . '"',
					'"' . $player->{last} . '"',
					'"' . $player->{boxname} . '"',
					($jersey == "" ? "null" : $jersey),
					"'" . $player->{rl} . "'",
					"'" . $player->{position} . "'"
				)) . ")";
				
			$sth = $dbh->prepare($roster_query);
			die ("MySQL Error: $dbh->$errmsg\n") unless defined($sth);
			$sth->execute() or die ("MySQL Error: $DBI::errstr\n$roster_query\n");
			$sth->finish();
		}
	}
}
1;
