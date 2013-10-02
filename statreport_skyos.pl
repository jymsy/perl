#!/usr/bin/perl -w
# create by yujinshi  2013-7-23

use DBI;
use POSIX;
use strict;
use File::Tail;
use Time::Local;

require './common.pl';

my ($dbhost, $dbname, $dbuser, $dbpswd, $dbport) = ("10.132.43.142","web_server_stat","pusher","skypush.22", 3306);
my ($Debug, $commander, $dbInterval, $table, $dbh) = (1, "jiangyumeng", 10);
my ($SkyOsCmd_Tb) = ("t_skyos_cmd_stat");

run_as_daemon();

my ($logFile,$cutByHour) = ("/data/skyos_log/stat/SkyOsStat.log", 1);

my @logfile = get_file();

Debug("logfile is $logfile[0] ");
my $ref = tie *LogF, "File::Tail",
(
    name        =>  $logfile[0],
    maxinterval =>  3, 
    interval    =>  1,  
    adjustafter =>  7,  
        resetafter  =>  1,  
    tail        =>  100, 
    ignore_nonexistant  =>  1,
    name_changes        => sub
    {
        if (-f $logfile[1] || (! -f $logfile[0]))
        {
            @logfile = get_file();
            return $logfile[0];
        }
        #else
        #{
        #       my $i;
        #       my $fileret;
        #       my $t = time;
        #       my ($_y, $_m, $_d, $_h) = (localtime($t))[5,4,3,2]; $_y += 1900; $_m += 1;
        #       for($i=1;$i<=12;$i++)
        #       {
        #               ($_y, $_m, $_d, $_h) = (localtime($t+$i*3600))[5,4,3,2]; $_y += 1900; $_m += 1;
        #               $fileret = "${logFile}.". sprintf "%04d-%02d-%02d_%02d00", $_y, $_m, $_d, $_h;
        #               if(-f $fileret)
        #               {
        #                        @logfile = get_file();
        #                        return $logfile[0];
        #               }
        #       }
        #}
    },
    reset_tail  =>  -1, 
);

my $sec = 61 - (localtime(time))[0];
Debug("alarm $sec first");
$SIG{ALRM} = \&thetimer; 
alarm $sec;
my $sigset = POSIX::SigSet->new(SIGALRM);

my ($idx, %h_statis) = (0);
while (<LogF>)
{
    my $line=$_;
                $line=~ s/\t\t/\t/g;
    my @row = split /\t| /, $line, -1;
    #Debug("unknown app: $row[2]");
    if("cmd_stat" eq $row[2])
    {   
            #[2013-09-10 10:49:00.715376]    cmd_stat  121.199.45.31   PushSvc.Login   2 1000
            my $prefix = $row[2];
            my $hostip = $row[3];
        my $cmd = $row[4];
        sigprocmask(SIG_BLOCK, $sigset);

                $h_statis{$prefix}{"$cmd|$hostip"}[0] =  $prefix;
                $h_statis{$prefix}{"$cmd|$hostip"}[1] =  $hostip;
                $h_statis{$prefix}{"$cmd|$hostip"}[2] =  $cmd;
                $h_statis{$prefix}{"$cmd|$hostip"}[4] += $row[5];
                $h_statis{$prefix}{"$cmd|$hostip"}[5] += $row[6];

        sigprocmask(SIG_UNBLOCK, $sigset);
    }
    else
    {
        # Debug "unknown app: $appname, skip";
        next;
    }
}

sub thetimer
{
    alarm 0;

    my $tmp = time;
    my ($sec, $min, $hour) = (localtime($tmp))[0, 1, 2];
    #print STDERR "dbInterval = $dbInterval ,now is   $hour:$min:$sec\n";
    if (0 == (60*$hour+$min) % $dbInterval)
    {
        if (0==$min)
        {
            if (0==$hour) {
                save_data("2360");
            }
            else {
                save_data(sprintf "%02d60", $hour-1);
            }
        }
        else
        {
            save_data(sprintf "%02d%02d", $hour, $min);
        }

        $sec = ($dbInterval-1)*60 - $sec + $tmp - time + 2;
        $sec = ( 0 >= $sec)?60:$sec;
    }
    else
    {
        $sec = 61 - $sec;
    }

    #Debug("alarm $sec");
    $SIG{ALRM}=\&thetimer;
    alarm $sec;
}

sub save_data
{
    my ($timestr) = @_;

    $SIG{CHLD} = 'IGNORE';
    my $pid = fork();
    while (0>$pid)
    {
        print STDERR "fork error when save data\n";
        qx(sleep 0.001);
        $pid = fork();
    }

    if (0<$pid)
    {
        # ¸¸½ø³Ì£¬ÍË³ö
        %h_statis = ();
        return 0;
    }

    my $sleep_sec = floor(rand(19)) + 1;
    #Debug("child $$ sleep $sleep_sec");
    sleep $sleep_sec;
        my $theday = get_the_date(int(("2360" eq $timestr)?-1:0));

each_app:
        foreach my $appName (keys %h_statis)
        {   
                $table = "t_skyos_$appName";
                #Debug("table is $table"); 
                push_crt_table();
        #Debug("table is $table");     

                my $sql = "delete from $table where f_date = '$theday' and f_tflag = '$timestr'";
                #Debug($sql);
                push_realdo($sql);

                my ($icount, $rv,  $col_start, $col_end, @res, @thearray) = (0);


                if( $table eq $SkyOsCmd_Tb)
                {   
                    #Debug("enter into SkyOsCmd_Tb");  
                        $sql = qq { replace into $table( f_date, f_tflag, f_interface, f_hostip, f_cmd,f_consumeTime,f_count) 
                                                           values ( '$theday', '$timestr', ?, ?, ?, ?, ?)};
                        foreach my $key (keys %{$h_statis{$appName}})
                        {   
                            $h_statis{$appName}{$key}[3] =$h_statis{$appName}{$key}[5]/$h_statis{$appName}{$key}[4];
                                push @{$thearray[$_]}, ($h_statis{$appName}{$key}[$_])?($h_statis{$appName}{$key}[$_]):0 foreach (0..4);
                                $icount++;
                        }
                        $col_start  = 0;
                        $col_end  = 4 ;
                } 
                else 
                {
            next each_app;
        }

                if (0 < $icount) 
            {
            my $sth = $dbh->prepare($sql) or next each_app;
            $sth->bind_param_array($_+1, $thearray[$_]) foreach ($col_start..$col_end);
            $rv = $sth->execute_array( {ArrayTupleStatus => \@res} );
            print("[$res[2]->[0]][$res[2]->[1]]\n") if (!$rv && ref $res[2]);
        }
        }
    exit 0;
}

sub push_db_conn
{
    $dbh = DBI->connect("dbi:mysql:${dbname}:${dbhost}:$dbport", $dbuser, $dbpswd) or die $!;
}

sub push_realdo
{
    my $query = shift;
    return 0 unless defined $query;

    if (!defined($dbh) || !$dbh->do($query))
    {
        push_db_conn();
        #debug("reconnect to the database");
        return (0+$dbh->do($query));
    }
    return 1;
}

sub push_crt_table
{
#debug("table:$table");
    if( $table eq $SkyOsCmd_Tb)
    {
                push_realdo (qq {
                    create table if not exists $table
                    (
                          f_date date not null,
                          f_tflag varchar (8)  not null,
                          f_interface varchar (128)  not null,
                          f_hostip varchar (20) not null,
                          f_cmd varchar (128)  not null,
                          f_consumeTime bigint NOT NULL,
                          f_count bigint NOT NULL,
                          primary KEY (f_date, f_tflag,f_interface,f_hostip,f_cmd)
                        )
                });
        }
}

sub get_file
{
    my @ret = ();
    if ($cutByHour)
    {
        my $t = time;
        my ($_y, $_m, $_d, $_h) = (localtime($t))[5,4,3,2]; $_y += 1900; $_m += 1;
        push @ret, "${logFile}.". sprintf "%04d-%02d-%02d_%02d00", $_y, $_m, $_d, $_h;

        ($_y, $_m, $_d, $_h) = (localtime($t+3600))[5,4,3,2]; $_y += 1900; $_m += 1;
        push @ret, "${logFile}.". sprintf "%04d-%02d-%02d_%02d00", $_y, $_m, $_d, $_h;
    }
    else
    {
        push @ret, "${logFile}.". get_the_date("-");
        push @ret, "${logFile}.". get_the_date(1, "-");
    }
    #Debug("check file tail, create filename");
    return @ret;
}
sub avg
{
    my @values = @_;
    return undef if (0==@values);

    my $total = 0;
    foreach(@values)
    {
        $total += $_;
    }
    return $total/@values;
}

sub stddev
{
    my @values = @_;
    return () if (0==@values);

    my $avg = avg(@values);
    my $total = 0;
    foreach (@values)
    {
        $total += ($_-$avg)*($_-$avg);
    }
    return ($avg, sqrt($total/@values));
}

sub todo
{
    my $cmd = shift;
    return unless defined $cmd;

    Debug($cmd);
    qx($cmd);
}

sub localip
{
    my $lbip = qx(/sbin/ifconfig eth1 |grep -i "inet addr" |awk '{print \$2}' |awk -F ":" '{print \$2}');
    chomp($lbip);
    return $lbip;
}
