#!/usr/bin/perl

use strict;
use warnings;

use DBI;
my $dbh = DBI->connect("dbi:SQLite:dbname=database.sqlite","","");
