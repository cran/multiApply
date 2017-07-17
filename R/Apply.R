#' Wrapper for Applying Atomic Functions to Arrays.
#'
#' The Apply function is an extension of the mapply function, which instead of taking lists of unidimensional objects as input, takes lists of multidimensional objects as input, which may have different numbers of dimensions and dimension lengths. The user can specify which dimensions of each array (or matrix) the function is to be applied over with the margins option. 
#' @param data A single object (vector, matrix or array) or a list of objects. They must be in the same order as expected by AtomicFun.
#' @param margins List of vectors containing the margins for the input objects to be split by. Or, if there is a single vector of margins specified and a list of objects in data, then the single set of margins is applied over all objects. These vectors can contain either integers specifying the dimension position, or characters corresponding to the dimension names. If both margins and inverse_margins are specified, margins takes priority over inverse_margins.
#' @param AtomicFun Function to be applied to the arrays.
#' @param ... Additional arguments to be used in the AtomicFun.
#' @param inverse_margins List of vectors containing the dimensions to be input into AtomicFun for each of the objects in the data. These vectors can contain either integers specifying the dimension position, or characters corresponding to the dimension names. If both margins and inverse_margins are specified, margins takes priority over inverse_margins.
#' @param parallel Logical, should the function be applied in parallel.
#' @param ncores The number of cores to use for parallel computation.
#' @details When using a single object as input, Apply is almost identical to the apply function. For multiple input objects, the output array will have dimensions equal to the dimensions specified in 'margins'.
#' @return Array or matrix or vector resulting from AtomicFun.
#' @references Wickham, H (2011), The Split-Apply-Combine Strategy for Data Analysis, Journal of Statistical Software.
#' @export
#' @examples
#' #Change in the rate of exceedance for two arrays, with different 
#' #dimensions, for some matrix of exceedances.
#' data = list(array(rnorm(2000), c(10,10,20)), array(rnorm(1000), c(10,10,10)), 
#'             array(rnorm(100), c(10, 10)))
#' test_fun <- function(x, y, z) {((sum(x > z) / (length(x))) / 
#'                                (sum(y > z) / (length(y)))) * 100}
#' margins = list(c(1, 2), c(1, 2), c(1,2))
#' test <- Apply(data, margins, AtomicFun = "test_fun")
Apply <- function(data, margins = NULL, AtomicFun, ..., inverse_margins = NULL, parallel = FALSE, ncores = NULL) {
  if (!is.list(data)) {
    data <- list(data)
  }
  if (!is.null(margins)) {
    inverse_margins <- NULL
  }
  if (!is.null(inverse_margins)) {
    if (!is.list(inverse_margins)) {
      inverse_margins <- rep(list(inverse_margins), length(data))
    }
    if (is.character(unlist(inverse_margins[1]))) {
      margins2 <- inverse_margins
      for (i in 1 : length(data)) {
        margins_new <- c()
        for (j in 1 : length(margins2[[i]])) {
          margins_new[j] <- which(names(dim(data[[i]])) == margins2[[i]][[j]])
        }
        inverse_margins[[i]] <- c(margins_new)
      }
    }
    for (i in 1 : length(data)) {       
      margins[[i]] <- c(1 :length(dim(data[[i]])))[-c(inverse_margins[[i]])]   
    }
  }
  if (!is.null(margins)) {
    if (!is.list(margins)) {
      margins <- rep(list(margins), length(data))
    }
  }
  if (is.character(unlist(margins[1])) && !is.null(margins)) {
    margins2 <- margins
    for (i in 1 : length(data)) {
      margins_new <- c()
      for (j in 1 : length(margins2[[i]])) {
        margins_new[j] <- which(names(dim(data[[i]])) == margins2[[i]][[j]])
      }
      margins[[i]] <- c(margins_new)
    }
  }
  if (!is.logical(parallel)) {
    stop("parallel must be logical")
  }
  names <- names(dim(data[[1]]))[margins[[1]]]
  input <- list()
  splatted_f <- splat(get(AtomicFun))
  if (!is.null(margins)) {
    .isolate <- function(data, margin_length, drop = TRUE) {
      eval(dim(environment()$data))
      structure(list(env = environment(), index = margin_length, subs = as.name("[")), 
                class = c("indexed_array"))
    }
    for (i in 1 : length(data)) {
      margin_length <- lapply(dim(data[[i]]), function(x) 1 : x)
      margin_length[-margins[[i]]] <- ""
      margin_length <- expand.grid(margin_length, KEEP.OUT.ATTRS = FALSE, 
                                   stringsAsFactors = FALSE)
      input[[i]] <- .isolate(data[[i]], margin_length)
    }
    dims <- dim(data[[1]])[margins[[1]]]
    i_max <- length(input[[1]])[1] / dims[[1]]
    k <- length(input[[1]]) / i_max
    if (parallel == TRUE) {
      if (is.null(ncores)) {
        ncores <- availableCores() - 1
      } else {
        ncores <- min(availableCores() - 1, ncores)
      }
      registerDoParallel(ncores)  
    }
    WrapperFun <- llply(1 : i_max, function(i)
      sapply((k * i - (k - 1)) : (k * i), function(x) splatted_f(lapply(input, `[[`, x),...), simplify = FALSE),
      .parallel = parallel)
    if (parallel == TRUE) {
      registerDoSEQ()
    }
    if (is.null(dim(WrapperFun[[1]][[1]]))) {
      WrapperFun <- array(as.numeric(unlist(WrapperFun)), dim=c(c(length((WrapperFun[[1]])[[1]])),
                                                                dim(data[[1]])[margins[[1]]]))
    } else {
      WrapperFun <- array(as.numeric(unlist(WrapperFun)), dim=c(c(dim(WrapperFun[[1]][[1]])),
                                                                dim(data[[1]])[margins[[1]]]))
    }
  } else {
    WrapperFun <- splatted_f(data, ...)
  }
  if (!is.null(dim(WrapperFun))) {
    names(dim(WrapperFun)) <- c(AtomicFun, names)
  }
  out <- WrapperFun
}



