#!/bin/bash

# Start and End dates extracted from the monday_groups_2024_2025.csv
start_dates=('20240101' '20240129' '20240226' '20240325' '20240422' '20240520' '20240617' '20240715' '20240812' '20240909' '20241007' '20241104' '20241202' '20241230' '20250127' '20250224' '20250324' '20250421')
end_dates=('20240128' '20240225' '20240324' '20240421' '20240519' '20240616' '20240714' '20240811' '20240908' '20241006' '20241103' '20241201' '20241229' '20250126' '20250223' '20250323' '20250420' '20250518')

# Loop through the lists and run the script in parallel
for i in ${!start_dates[@]}; do
    start_date=${start_dates[$i]}
    end_date=${end_dates[$i]}

    # Run the Python script in the background
    python3 oxxo_sellout_preaggregates.py --start_date $sta
    rt_date --end_date $end_date &
done

# Wait for all background jobs to finish
wait
