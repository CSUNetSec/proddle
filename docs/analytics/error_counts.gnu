set title 'Failure Error Codes' font 'Helvetica Bold, 14'
set xlabel 'Error Code' font 'Helvetica Bold, 12'
set ylabel 'Count' font 'Helvetica Bold, 12'

set grid ytics
set yrang [0.5:25000]
set logscale y

set style data histogram
set style fill solid
set boxwidth 0.9
set xtic scale 0
plot 'error_counts.csv' using 3:xtic(2) ti 'Full Dataset' lc rgb '#4D4D4D', \
    '' using 4 ti 'Removed 100% Failure Domains' lc rgb '#5DA5DA'
