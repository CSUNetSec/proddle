set title 'Failures Grouped by Error and Domain Failure Rate' font 'Helvetica Bold, 14'
set xlabel 'Domain Failure Rate' font 'Helvetica Bold, 12'
set ylabel 'Failure Count' font 'Helvetica Bold, 12'

set grid ytics
set logscale y
set key top left
set bmargin 3
set yrang [5:25000]

set style data histogram
set style histogram cluster gap 3
set style fill solid
set boxwidth 0.9
set xtic scale 0
plot 'failures_grouped_by_error_and_failure_rate-1.csv' using 3:xtic(2) ti "Couldn't resolve host name" lc rgb '#4D4D4D', \
    '' using 4 ti "Couldn't connect to server" lc rgb '#5DA5DA', \
    '' using 6 ti 'Failed writing received data' lc rgb '#FAA43A', \
    '' using 7 ti 'Timeout was reached' lc rgb '#60BD68', \
    '' using 8 ti 'SSL connect error' lc rgb '#F17CB0', \
    '' using 10 ti 'Server returned nothing' lc rgb '#B2912F', \
    '' using 11 ti 'Failure when receiving data' lc rgb '#B276B2', \
    '' using 12 ti 'Peer certificate authentication' lc rgb '#DECF3F', \
    '' using 14 ti 'Other' lc rgb '#F15854'
    #'' using 9 ti 'SSL peer certificate not OK', \
    #'' using 5 ti 'Transfered a partial file', \
    #'' using 13 ti 'Unrecognized content/encoding'
