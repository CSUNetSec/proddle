set title 'Failure Rates for 617 Domains With Failures' font 'Helvetica Bold, 14'
set xlabel 'Domain' font 'Helvetica Bold, 12'
set xrang [0:617]
set ylabel 'Failure Rate (Percent)' font 'Helvetica Bold, 12'
set yrang [0:110]

set grid ytics

set boxwidth 0.5
set style fill solid
plot 'domain_failure_rates_sorted_filtered.csv' using 1:($6 / ($3 + $6) * 100) ti 'failure rates' lc rgb '#5DA5DA' with boxes
