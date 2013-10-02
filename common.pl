#!/usr/bin/perl -w
use DBI;
use POSIX;
use Time::Local;

my ($g_user, $g_type, $g_log, $g_prog) = ("", 111, "/tmp/yujinshi.log");

BEGIN
{
    $SIG{__WARN__} = \&to_warn;
    $SIG{__DIE__}  = $SIG{TERM}= $SIG{INT}=$SIG{QUIT}=$SIG{USR1}=$SIG{USR2}= \&to_die;
}

sub to_warn {   print STDERR "__WARN__: @_";    }

sub to_die
{
    $g_prog = "" unless defined $g_prog;

    print STDERR "${g_prog}__DIE__: @_";
    CORE::exit;
}

sub set_alarm
{
    $g_prog = $0;
    my $_pos  = rindex($g_prog, '/');
    $g_prog = substr($g_prog, $_pos+1) unless (-1 == $_pos);

    my $log = $g_prog;
    $log =~ s/\..*/\.log/;
    set_log_file("../log/$log");

    my ($user, $type) = @_;
    if (defined($type) && $type =~ m/^(\d+)$/)
    {
        $g_type = $1;
    }

    $g_user = $user if (defined($user));
}

# ÉèÎª¾ø¶ÔÂ·¾¶£¬ÒÔÃâ³ÌÐòÖÐchdirÓ°Ïì
sub set_log_file
{
    my $path = shift;
    if ($path =~ m/^\//) {
        $g_log = $path;
    }
    else {
        # use Cwd;
        $g_log = getcwd(). "/$path";
    }
}

sub Debug
{
    my $info = join(" ", @_);
    $info = "" unless defined $info;
    my ($_y, $_m, $_d, $_h, $_i, $_s) = (localtime(time))[5,4,3,2,1,0];
    $_y += 1900; $_m += 1;

    my $fi = sprintf "[%04d-%02d-%02d %02d:%02d:%02d]", $_y, $_m, $_d, $_h, $_i, $_s;
    if (defined($g_log) && open(LOGF, ">>$g_log"))
    {
        print LOGF "$fi $info\n";
        close(LOGF);
    }

    print STDERR "$fi $info\n";
}

sub is_leap
{
    my ($year) = shift;
    return 0 if ($year%4);
    return 0 if (0==$year%100 && $year%400);
    return 1;
}

sub get_month_days
{
    my ($year, $month) = @_;
    if (1==@_ && 6==length($year))
    {
        $month = substr($year, -2);
        $year = substr($year, 0,4);
    }
    my @month_days = (0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31);
    my $mday = $month_days[$month];
    $mday += is_leap($year, $month) if ($month == 2);
    return $mday;
}

sub delta_month
{
    my ($ym1, $ym2) = @_;
    return undef unless defined $ym1;
    unless (defined $ym2)
    {
        $ym2 = $ym1;
        $ym1 = substr(get_the_date(),0,6);
    }

    my ($t1, $t2);
    if ($ym1 =~ m/^[1-9]\d{5}$/) {
        $t1 = 12*substr($ym1, 0, 4)+substr($ym1, -2);
    }
    elsif ($ym1 =~ m/^(\d{4})(\D+)(\d{2})/) {
        $t1 = 12*$1+$3;
    }
    else {
        return undef;
    }

    if ($ym2 =~ m/^[1-9]\d{5}$/) {
        $t2 = 12*substr($ym2, 0, 4)+substr($ym2, -2);
    }
    elsif ($ym2 =~ m/^(\d{4})(\D+)(\d{2})/) {
        $t2 = 12*$1+$3;
    }
    else {
        return undef;
    }

    return $t1 - $t2;
}

sub get_next_month
{
    my ($year, $month) = @_;

    my $flag = 0;
    if (1==@_ && 6==length($year))
    {
        $month = substr($year, -2);
        $year = substr($year, 0,4);
        $flag = 1;
    }

    if (12 <= $month)
    {
        $year++;
        $month = 1;
    }
    else {
        $month++;
    }

    return sprintf "%04d%02d", $year, $month if ($flag);
    return ($year, $month);
}

sub get_prev_month
{
    my ($year, $month) = @_;
    my $flag = 0;
    if (1==@_ && 6==length($year))
    {
        $month = substr($year, -2);
        $year = substr($year, 0,4);
        $flag = 1;
    }

    if (1 >= $month)
    {
        $year--;
        $month = 12;
    }
    else {
        $month--;
    }

    return sprintf "%04d%02d", $year, $month if ($flag);
    return ($year, $month);
}

sub get_the_month
{
    my ($ic, $p1, $p2) = ("");
    my $ym = substr(get_the_date(), 0, 6);
    my $t = 12*substr($ym, 0, 4)+substr($ym, -2);

    if (1==@_)
    {
        #3 one, interval or delimti
        $p1 = shift;
        if ($p1 =~ m/^[-+]?\d+$/)
        {
            # yyyymm¸ñÊ½
            if ($p1 =~ m/^[1-9]\d{5}$/) {
                $t = 12*substr($p1, 0, 4)+substr($p1, -2);
            }
            else {
                $t += $p1 if (abs($p1)<12000);
            }
        }
        else {
            $ic = $p1;
        }
    }
    elsif (2==@_)
    {
        ($p1, $p2) = @_;
        if ($p1 =~ m/^[-+]?\d+$/)
        {
            # yyyymm¸ñÊ½
            if ($p1 =~ m/^[1-9]\d{5}$/) {
                $t = 12*substr($p1, 0, 4)+substr($p1, -2);
            }
            else {
                $t += $p1 if (abs($p1)<12000);
            }
        }
        elsif ($p1 =~ m/^(\d{4})(\D+)(\d{2})/)
        {
            $t = 12*$1+$3;
            $ic = $2;
        }

        if ($p2 =~ m/^[-+]?\d+$/) {
            $t += $p2;
        }
        else {
            $ic = $p2;
        }
    }
    elsif (3==@_)
    {
        ($p1, $p2, $ic) = @_;
        if ($p1 =~ m/^[1-9]\d{5}$/) {
            $t = 12*substr($p1, 0, 4)+substr($p1, -2);
        }
        elsif ($p1 =~ m/^(\d{4})(\D+)(\d{2})/) {
            $t = 12*$1+$3;
        }

        $t += $p2 if ($p2 =~ m/^[-+]?\d+$/);
    }

    my $year = int($t/12);
    my $month = $t%12;
    if (0==$month)
    {
        $year--;
        $month = 12;
    }

    return sprintf "%04d${ic}%02d", $year, $month;
}

sub get_the_date
{
    my ($ic, $t, $year, $month, $day, $p1, $p2) = ("", time);
    if (1==@_)
    {
        #3 one, interval or delimti
        $p1 = shift;
        if ($p1 =~ m/^[-+]?\d+$/)
        {
            # ´óÊý×ÖÔòÎªÃëÊý
            if ($p1 =~ m/^[1-9]\d{9}$/) {
                $t = $p1;
            }
            else {
                $t += $p1 * 3600*24;
            }
        }
        else {
            $ic = $p1;
        }
    }
    elsif (2==@_)
    {
        ($p1, $p2) = @_;
        if ($p1 =~ m/^[-+]?\d+$/)
        {
            if ($p1 =~ m/^\d{8}$/)
            {
                $year  = substr($p1, 0, 4);
                $month = substr($p1, 4, 2);
                $day   = substr($p1, 6, 2);
                $t = timelocal(0, 0, 0, $day, $month-1, $year-1900);
            }
            elsif ($p1 =~ m/^[1-9]\d{9}$/) {
                $t = $p1;
            }
            else {
                $t += $p1 * 3600*24;
            }
        }
        elsif ($p1 =~ m/^(\d{4})(\D+)(\d{2})\D+(\d{2})/)
        {
            $t = timelocal(0, 0, 0, $4, $3 - 1, $1 -1900);
            $ic = $2;
        }

        if ($p2 =~ m/^[-+]?\d+$/) {
            $t += $p2 * 3600*24;
        }
        else {
            $ic = $p2;
        }
    }
    elsif (3==@_)
    {
        ($p1, $p2, $ic) = @_;
        if ($p1 =~ m/^\d{8}$/)
        {
            $year  = substr($p1, 0, 4);
            $month = substr($p1, 4, 2);
            $day   = substr($p1, 6, 2);
            $t = timelocal(0, 0, 0, $day, $month-1, $year-1900);
        }
        elsif ($p1 =~ m/^[1-9]\d{9}$/) {
            $t = $p1;
        }
        elsif ($p1 =~ m/^(\d{4})(\D+)(\d{2})\D+(\d{2})/) {
            $t = timelocal(0, 0, 0, $4, $3 - 1, $1 -1900);
        }

        $t += $p2 * 3600*24 if ($p2 =~ m/^[-+]?\d+$/);
    }

    ($year, $month, $day) =  (localtime($t))[5,4,3];
    $year += 1900;    $month++;
    return sprintf "%04d${ic}%02d${ic}%02d", $year, $month, $day;
}

sub get_the_time
{
    my $ic = "";
    $ic = shift if (1 == @_);
    my ($h, $m, $s) = (localtime(time))[2,1,0];
    return (sprintf "%02d$ic%02d$ic%02d", $h, $m, $s);
}

sub timestamp
{
    my $len = shift;
    $len = 14 if (!defined($len) || 1>$len ||14<$len);

    my ($y, $mm, $dd, $h, $m, $s) = (localtime(time))[5,4,3,2,1,0];
    $y += 1900; $mm ++;
    my $retstr = sprintf "%04d%02d%02d%02d%02d%02d", $y, $mm, $dd, $h, $m, $s;
    return substr($retstr, -1*$len);
}

sub run_as_daemon
{
    my $pid = fork();
    if ($pid)       { CORE::exit(0); }
    elsif (0>$pid)  { CORE::exit(1); }

    $pid = fork();
    if ($pid)       { CORE::exit(0); }
    elsif (0>$pid)  { CORE::exit(1); }
}

sub is_table_exist
{
    my ($dbh, $ts_name, $type, $dblink) = @_;
    return -1 if (!defined($ts_name));
    $type = "oracle" if (!defined($type));
    return -2 if ("oracle" ne $type && "mysql" ne $type);
    if ("oracle" eq $type && defined($dblink) && 3<= length($dblink))
    {
        return -3 if (0 != check_db_link($dbh, $dblink));
        $dblink = "\@$dblink";
    }
    else
    {
        $dblink = "";
    }

    my $ret = 1;
    my $query = qq {
        select  count(1)
        from    tab$dblink
        where   TABTYPE = 'TABLE'
            and tname = upper('$ts_name')
    };
    $query = "show tables like '$ts_name' " if ("mysql" eq $type);
    my $sth = $dbh->prepare($query) or die "$!";
    $sth->execute or die "$!";
    my @result=$sth->fetchrow_array;
    $sth->finish;

    $ret = 0 if (defined($result[0]) && "oracle" eq $type && 1 == $result[0]);
    $ret = 0 if (defined($result[0]) && "mysql" eq $type && $ts_name eq $result[0]);
    return $ret;
}

sub is_db_exist
{
    my ($dbh, $ts_name) = @_;
    return -1 if (!defined($ts_name));

    my $ret = 1;
    $query = "show databases like '$ts_name' ";
    my $sth = $dbh->prepare($query) or die "$!";
    $sth->execute or die "$!";
    while (my @result=$sth->fetchrow_array)
    {
        $ret = 0 if (defined($result[0]) && $ts_name eq $result[0]);
        last;
    }
    $sth->finish;
    return $ret;
}
