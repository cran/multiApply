# Function to permute arrays of non-atomic elements (e.g. POSIXct)
.aperm2 <- function(x, new_order) {
  old_dims <- dim(x)
  if (is.numeric(x)) {
    x <- aperm(x, new_order)
  } else {
    y <- array(1:length(x), dim = dim(x))
    y <- aperm(y, new_order)
    x <- x[as.vector(y)]
  }
  dim(x) <- old_dims[new_order]
  x
}
