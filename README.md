# Spark Coding Challenge

## Repo Contents

├── README.md
├── tests
│   ├── run_tests.sh
│   ├── test1
│   │   ├── expected.txt
│   │   └── input.txt
│   ├── test2
│   │   ├── expected.txt
│   │   └── input.txt
│   ├── test3
│   │   ├── expected.txt
│   │   └── input.txt
│   └── test4
│       ├── expected.txt
│       └── input.txt
└── validate_ipv4.py

- `validate_ipv4.py` is the Spark python script designed to validate IPv4 addresses according to the problem description below.
- `run_tests.sh` is a Bash script to run the tests and print whether the test passes (i.e. whether the expected output matches the actual output).
- Each test has its own directory. The `input.txt` is a csv file that contains the Spark DataFrame with IPv4 addresses to validate. The `expected.txt` is a csv file that contains the columns from `input.txt` with valid IPv4 addresses.

## Problem Statement
Imagine that you have a table available to you as a Spark Dataframe. Each entry is an IPv4 Address. Your task is to return the columns where all the values in these columns are correctly formatted. The correct formats are: y.y.y.y, y-y-y-y and y:y:y:y" (0<=y<=255) and nothing else. You can assume that your task is to validate the format and not the content (that means 0.0.0.0 and 255.255.255.255 are all valid values).

The purpose of this challenge is to help you better understand Spark concepts such as transformations and actions. Please focus on shuffle operations when you try to optimize your solution. Generally speaking, scanning the data too many times/lots of shuffling should be avoided.

### Example

#### Input

|Column 1|Column 2|Column 3|Column 4|Column 5|
|---|---|---|---|---|
|11.22.33.44|11-22-33-44|11.22.33.44|11-22-33-44|11.22.33.44|
|55.66.77.88|55-66-77-88|55.66.77|55-66-777-88|null|
|99.00.11.22|99-00-11-22|88-99-00-11|99-00-11-22|null

#### Output
 
|Column 1|Column 2|
|---|---|
|11.22.33.44|11-22-33-44|
|55.66.77.88|55-66-77-88|
|99.00.11.22|99-00-11-22|

## Solution
### Overview
For each column, the entries in the column are checked against a regular expression for validity. The number of valid expressions for a given column are counted. If the column number of valid expressions is less than the number of entries in the column, the column is marked for deletion.

After passing through each column, the marked columns are dropped and the resulting DataFrame is output to a file as a csv for inspection or further proccessing.

### Regular Expression Explanation

The regular expression used in the solution `validate_ipv4.py` is rather unweidly and requires some unpacking. The base for the solution is the need to validate whether a number is between 0 and 255. In order to create the regular expression, we break down the range into the following subranges: 0-9, 10-89, 90-99, 100-199, 200-249, and 250-255. We join these ranges with the OR operator (`|`) to yield `[0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]`.

Next we need to allow for one or more leading zeros as seen in the example above. We wrap the expression above in parenthesis to create a word block that will match 0-255, and then prepend the expression `0*`, which tells the expression to match one or more zeros in the front. This yields the following regular expression: `0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])`

In order to match an IPv4 address, we need to check for four numbers connected by :, -, or . characters. Since these characters have special meaning in regular expressions, we should escape them with a backslash (`\\:`, `\\-` and `\\.`). Combining this with the above, we obtain the expressions below, each designed to match one of the three formats for the IPv4.

    (0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\:(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\:(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\:(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))
    
    (0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\-(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\-(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\-(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))
    
    (0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\.(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\.(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\.(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))
    
Furthermore, since we only want to match the IPv4 and not when it is embedded in another string (e.g. `aaaa123.123.123.123bbb`), we need to tell the program that we want the above statements to match successfully when the statement begins and ends with a valid IPv4. Therefore, we prepend a `^` character and append a `$` character to the expressions above that marks the beginning and end of a string, respectively. This yeilds the new expressions below.

    ^(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\:(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\:(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\:(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))$
    
    ^(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\-(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\-(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\-(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))$
    
    ^(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\.(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\.(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\.(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))$
    
Now, in order to avoid three separate function calls, we concatentate the above with the OR operator, which generates the demonic regular expression below that is seen in the solution file.

    ^(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\:(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\:(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\:(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))$|^(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\-(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\-(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\-(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))$|^(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\.(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\.(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\.(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))$
