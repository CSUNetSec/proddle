set title 'Failure Counts by Domain' font 'Helvetica Bold, 14'
set xlabel 'Domain' font 'Helvetica Bold, 12'
set xrang [0:617]
set ylabel 'Failure Count' font 'Helvetica Bold, 12'
set yrang [0:170]

set grid ytics

set boxwidth 0.5
set style fill solid
plot 'domain_failures.csv' using 1:3 ti 'failures' linecolor '#808080' with boxes
