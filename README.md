## multiApply 
[![CRAN version](http://www.r-pkg.org/badges/version/multiApply)](https://CRAN.R-project.org/package=multiApply)
![coverage report](https://earth.bsc.es/gitlab/ces/multiApply/badges/master/coverage.svg) 
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![CRAN RStudio Downloads](https://cranlogs.r-pkg.org/badges/multiApply)](https://CRAN.R-project.org/package=multiApply)

This package includes the function `Apply` as its only function. It extends the `apply` function to applications in which a function needs to be applied simultaneously over multiple input arrays. Although this can be done manually with for loops and calls to the base `apply` function, it can often be a challenging task which can easily result in error-prone or memory-inefficient code.

A very simple example follows showing the kind of situation where `Apply` can be useful: imagine you have two arrays, each containing five 2x2 matrices, and you want to perform the multiplication of each of the five pairs of matrices. Next, one of the best ways to do this with base R (plus some helper libraries):

```r
library(plyr)
library(abind)

A <- array(1:20, c(5, 2, 2))
B <- array(1:20, c(5, 2, 2))

D <- aaply(X = abind(A, B, along = 4), 
           MARGINS = 1, 
           FUN = function(x) x[,,1] %*% x[,,2])
```

Since the choosen use case is very simple, this solution is not excessively complex, but the complexity would increase as the function to apply required additional dimensions or inputs, and would be unapplicable if multiple outputs were to be returned. In addition, the function to apply (matrix multiplication) had to be redefined for this particular case (multiplication of the first matrix along the third dimension by the second along the third dimension).

Next, an example of how to reach the same results using `Apply`:

```r
library(multiApply)

A <- array(1:20, c(5, 2, 2))
B <- array(1:20, c(5, 2, 2))

D <- Apply(data = list(A, B), 
           target_dims = c(2, 3), 
           fun = "%*%")$output1
```

This solution takes half the time to complete (as measured with `microbenchmark` with inputs of different sizes), and is cleaner and extensible to functions receiving any number of inputs with any number of dimensions, or returning any number of outputs. Although the peak RAM usage (as measured with `peakRAM`) of both solutions in this example is about the same, it is challenging to avoid memory duplications when using custom code in more complex applications, and can usually require hours of dedication. `Apply` scales well to large inputs and has been designed to be fast and avoid memory duplications.

Additionally, multi-code computation can be enabled via the `ncores` parameter, as shown next. Although in this minimalist example using multi-core would make the execution slower, in applications where the inputs are larger the wall-clock time is reduced dramatically.

```r
D <- Apply(data = list(A, B),
           target_dims = c(2, 3),
           fun = "%*%",
           ncores = 4)$output1
```

In contrast to `apply` and variants, this package suggests the use of 'target dimensions' as opposite to the 'margins' for specifying the dimensions relevant to the function to be applied. Additionally, it supports functions returning multiple vector or arrays, and can transparently uses multi-core.

### Installation

In order to install and load the latest published version of the package on CRAN, you can run the following lines in your R session:

```r
install.packages('multiApply')
library(multiApply)
```

### How to use

This package consistis in a single function, `Apply`, which is used in a similar fashion as the base `apply`. Full documentation can be found in `?Apply`.

A simple example is provided next. In this example, we have two data arrays. The first, with information on the number of items sold in 5 different stores (located in different countries) during the past 1000 days, for 200 different items. The second, with information on the price point for each item in each store.

The example shows how to compute the total income for each of the stores, straightforwardly combining the input data arrays, by automatically applying repeatedly the 'atomic' function that performs only the essential calculations for a single case.

```r
dims <- c(store = 5, item = 200, day = 1000)
sales_amount <- array(rnorm(prod(dims)), dims)

dims <- c(store = 5, item = 200)
sales_price <- array(rnorm(prod(dims)), dims)

income_function <- function(x, y) {
  # Expected inputs:
  #  x: array with dimensions (item, day)
  #  y: price point vector with dimension (item)
  sum(rowSums(x) * y)
}

income <- Apply(data = list(sales_amount, sales_price),
                target_dims = list(c('item', 'day'), 'item'), 
                income_function)

dim(income$output1)
# store
#     5
```
