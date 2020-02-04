#!/usr/bin/perl -W
# 20140626 schmutzr@post.ch


# TODO: - extract field-id's from first data-file line
#       - field-selection by numeric id, ranges, collections (like cut -c|f)
# BUGS: - aggregate-function is not invoked on single-member aggregates, ie "count" in the current implementation fails

use Getopt::Long::Descriptive;

my %aggregate_templates = ( # note: keys(%aggregate_templates) is used in optionparsing/usage message
  'sum'         => '$a+$b',
  'avg'         => '($a*$c+$b)/($c+1)',
  'list'        => 'join(",", $a, $b)',
  'ulist'       => '%k = map {$_=>1} (split(/,/,$a), $b); join(",", sort keys %k)',
  'max'         => '($a>$b)?$a:$b',
  'min'         => '($a<$b)?$a:$b',
  'count'       => '$c+1',
);



my ($opt, $usage) = describe_options(
  '%c %o [file... or stdin]',
  [ 'separator|s=s'     =>      "field separator (regex allowed), defaults to \";\""  , { default=>";"} ],
  [ 'infields|i=s'      =>      "comma or whitespace (quote) separated list of column names of input, defaults to first line of input-data",    { "required"=>1 } ],
  [ 'key|k=s'           =>      "comma or whitespace (quote) separated list of columns used as aggregate key",  { "required"=>1 } ],
  [ 'aggregate|a=s'     =>      "comma or whitespace (quote) separated list of columns to aggregate",   { "required"=>1 } ],
  [ 'function|f=s'      =>      "function used to aggregate, one of: " . join(", ", keys %aggregate_templates) . " or a perl subexpression taking two mandatory (\$a, \$b) and one optional (\$c) argument: \$a=so
far aggregated value, \$b=new input field value, \$c=count of aggregations (excluding new field value). eg. \"(\$a>\$b) ? \$a : \$b\" for max or \"(\$a*\$c+\$b)/(\$c+1)\" for avg",      { "required"=>1 } ],
  [],
  [ 'verbose|v'         =>      "print extra stuff"            ],
  [ 'help|h'            =>      "print usage message and exit" ],
);




# helpers
sub zipwith { # func, aref, aref, accumulatorcount -> a(ref?) | resulting vector length is min(|a|,|b|)
  my($f, $a, $b, $c) = @_;
  my(@result) = ();

  my $minlength = ($#{$a}>$#{$b}) ? $#{$b} : $#{$a};

  foreach my $index (0..$minlength) {
    push(@result, &{$f}(${$a}[$index], ${$b}[$index], $c));
  }
  return [ @result ];
}

##
# M A I N
#


# option-parsing
print($usage->text), exit if $opt->help;
my @fields    = split(/[,\s]+/, $opt->infields);
my @keyfields = split(/[,\s]+/, $opt->key);
my @aggfields = split(/[,\s]+/, $opt->aggregate);
my $funcarg   = (exists $aggregate_templates{$opt->function}) ? $aggregate_templates{$opt->function} : $opt->function;
my $sep = $opt->separator;
my $glue = "#";
my $func = sub { my($a, $b, $c)=@_; eval($funcarg) };
print STDERR "# aggregate: keys=(" . join(",", @keyfields) . "), aggregates=(" . join(",", @aggfields) . "), func='$funcarg'\n" if $opt->verbose;


# processing
my %aggregate = ();

while(<>) {
  chomp;
  @data{@fields} = split($sep);

  my $key    = join($glue, @data{@keyfields});
  my @values = @data{@aggfields};

  if(exists($aggregate{$key})) {
    $aggregate{$key}->{'data'} = zipwith( $func, $aggregate{$key}->{'data'}, [ @values ], $aggregate{$key}->{'count'});
    $aggregate{$key}->{'count'}++;
  } else {
    $aggregate{$key}->{'data'}  = [ @values ];
    $aggregate{$key}->{'count'} = 1;
  }

}

# report
foreach my $key (keys %aggregate) {
  print join($sep, split($glue, $key), @{$aggregate{$key}->{'data'}}) . "\n";
}
