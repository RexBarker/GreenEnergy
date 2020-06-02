# PriceEnergy Data:

Scripts gather price and energy data based on following arguments

- **collect_curr_year.sh**  --> gathers data for current year (obtained from POSIX 'date' function)
  - arg 1: keyword [solar, wind, air]
  - arg 2: start month (M)
  - arg 3: end month (M)
  
- **collect_year.sh** --> gathers data for given year
  - arg 1: keyword [solar, wind, air]
  - arg 2: year (YYYY)
  - arg 3: start month (M)
  - arg 4: end month (M)
