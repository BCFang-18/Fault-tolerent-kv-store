
#!/bin/bash

# Redirect all subsequent output to a log file
exec > log4a.txt 2>&1

# Check for valid input
if [ "$#" -ne 2 ] || ! [ "$1" -eq "$1" ] 2> /dev/null || ! [ "$2" -eq "$2" ] 2> /dev/null; then
  echo "Usage: $0 <number_of_runs> <max_jobs>" >&2
  exit 1
fi

# Number of times to run the command
n="$1"

# Max number of parallel jobs.
max_jobs="$2"

# Counters for pass and fail. These will be written to temporary files because of parallel execution.
pass_file=$(mktemp)
fail_file=$(mktemp)

echo 0 > "$pass_file"
echo 0 > "$fail_file"

# Function to be exported for parallel to use
do_test() {
    if go test -race; then
      # If command passes, increment pass_count
      echo "Run $1: PASS"
      echo $(( $(cat "$pass_file") + 1 )) > "$pass_file"
    else
      # If command fails, increment fail_count
      echo "Run $1: FAIL"
      echo $(( $(cat "$fail_file") + 1 )) > "$fail_file"
    fi
}
export -f do_test
export pass_file
export fail_file

# Using parallel to run the tests
seq "$n" | parallel -j "$max_jobs" do_test {}

# Capture the final counts
pass_count=$(cat "$pass_file")
fail_count=$(cat "$fail_file")

# Cleanup temp files
rm "$pass_file" "$fail_file"

# Display the results
echo "========================"
echo "Total runs: $n"
echo "Max parallel jobs: $max_jobs"
echo "Total PASS: $pass_count"
echo "Total FAIL: $fail_count"
