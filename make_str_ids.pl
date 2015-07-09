#!/usr/bin/perl -l

use strict;
use warnings;

my $header = <STDIN>;
chomp $header;
my @HF = split /\t/, $header;
my @id_mask = map { ($_ =~ m!ID$!) ? 1 : 0} @HF;
print $header;


while(<STDIN>) {
  chomp;
  
  my @F = split /\t/;
  print join "\t", map {if ($id_mask[$_]) { $_ = sprintf("%09d", $F[$_]) } else { $_ = $F[$_] } $_ }  0..$#F;  
}
