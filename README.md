# Spark Projet - ESGI 2022

By **[Maxime d'Harboullé](https://github.com/anonymax25")** and **[Julien Da Corte](https://github.com/Juliien)** (5AL1)

## Prerequisites

- Have spark, pyspark and python
- Get the dataset [here](https://www.kaggle.com/dhruvildave/github-commit-messages-dataset/version/3) and place it in the data folder as so: ```data/full.csv```

## Expected outputs

```bash
$ python main.py

Question 1                                                                      
+--------------------+-------+                                                  
|                repo|commits|
+--------------------+-------+
|         openbsd/src| 103906|
|      rust-lang/rust|  77696|
|    microsoft/vscode|  65518|
| freebsd/freebsd-src|  64103|
|      python/cpython|  63910|
|         apple/swift|  45756|
|kubernetes/kubern...|  41480|
|     rstudio/rstudio|  29384|
|       opencv/opencv|  25772|
|microsoft/TypeScript|  22017|
+--------------------+-------+

Question 2
+--------------------+-------+                                                  
|              author|commits|
+--------------------+-------+
|Matei Zaharia <ma...|    683|
+--------------------+-------+

Question 3
+--------------------+-------+                                                  
|              author|commits|
+--------------------+-------+
|Dongjoon Hyun <do...|      2|
|Sean Owen <srowen...|      1|
|Wenchen Fan <wenc...|      1|
+--------------------+-------+

Question 4
+-------+------+                                                                
|  words| count|
+-------+------+
|    fix|475785|
|    add|465963|
|  merge|302980|
| remove|260543|
|request|144940|
| update|141230|
|   pull|140033|
|support|129746|
|   test|109820|
|   make|109382|
+-------+------+

--- 105.15760445594788 secondes ---
```
