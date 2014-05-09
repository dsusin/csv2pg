#!/usr/bin/perl

use strict;
use warnings;
use XML::Simple;
use DBI;
use Data::Dumper;

sub spin {
	print [qw{/ - \ |}]->[$_[0]++]."\033[1D";
	$_[0]%=4;
}

if(scalar @ARGV!=2){
  print "Bad parameters\n";
  exit 0;
}

my $schema=XMLin($ARGV[0]);
my $datafile=$ARGV[1];

print Dumper $schema;
open my $fh, "<", $datafile or die $!;

my $conn=DBI->connect("DBI:Pg:dbname=".$schema->{database}->{dbname}) or die $DBI::errstr;
print "Connected to DB\n\n";

my $skipped=0;

while(<$fh>){
	if(exists $schema->{csv}->{skiprows}){
		if($skipped<$schema->{csv}->{skiprows}){
			$skipped++;
			next;
		}
	}

	chomp;
	my @line=split /$schema->{csv}->{separator}/;

	my $insert="INSERT INTO ".$schema->{database}->{table}."(";

	for(my $i=0; $i<scalar @line; $i++){
		if(exists $schema->{fields}->{"c$i"}){
			if($schema->{fields}->{"c$i"}->{included} ne "1"){
				next;
			}
		}
		else{
			next;
		}
		my $field=$schema->{fields}->{"c$i"};

		$insert.=$field->{fieldname}.",";
	}
	$insert=~s/,$/) VALUES(/;

	for(my $i=0; $i<scalar @line; $i++){
		if(exists $schema->{fields}->{"c$i"}){
			if($schema->{fields}->{"c$i"}->{included} ne "1"){
				next;
			}
		}
		else{
			next;
		}
		my $field=$schema->{fields}->{"c$i"};

		if($field->{datatype} eq "text"){
			$line[$i]=~s/'/''/g; #escape single quotes
				$line[$i]="E'".$line[$i]."'";
		}
		if(!$line[$i] or $line[$i] eq "''"){
			$line[$i]="null";
		}
		if($field->{datatype} eq "boolean"){
			if($line[$i] ne "null"){
				$line[$i].="::bool";
			}
		}
		if($field->{datatype} eq "timestamp"){
			if($line[$i] ne "null"){
				$line[$i]="to_timestamp(".$line[$i].")";
			}
		}
		
		$insert.=$line[$i].",";
	}

	$insert=~s/,$/);/;

#	print $insert, "\n";
	my $sth=$conn->prepare($insert);
	my $res=$sth->execute() or die $_;
    
}

$conn->disconnect();
