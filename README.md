csv2pg
======

Flexible csv to postgresql script. 

Many features missing, adding them on a need basis (datetime format string, for instance).

Create a xml template for each csv/table, sample included.

It's slow as it uses separate inserts instead of copy.

Copy should be used in cases of many rows (>100.000?), having in mind
its drawbacks.
