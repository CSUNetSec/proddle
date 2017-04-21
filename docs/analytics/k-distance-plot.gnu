set title 'K-Distance Plot' font 'Helvetica Bold, 14'
set xlabel 'K-Distances Points' font 'Helvetica Bold, 12'
set ylabel 'Duration (Hours)' font 'Helvetica Bold, 12'

unset key
set grid ytics
set style data line

plot '2017.04.03-2017.04.10-100-min-2.csv' using 2:xtic(1) lc rgb '#4D4D4D'
