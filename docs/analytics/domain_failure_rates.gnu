set title 'Failure Rates by Domain' font 'Helvetica Bold, 14'
set xlabel 'Domain' font 'Helvetica Bold, 12'
set xrang [0:10000]
set ylabel 'Failure Rate (Percent)' font 'Helvetica Bold, 12'
set yrang [0:110]

set grid ytics

set boxwidth 0.05
set style fill solid
plot 'domain_failure_rates.csv' using 1:($6 / ($3 + $6) * 100) ti 'failure rates' lc rgb '#5DA5DA' with boxes
