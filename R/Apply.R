#' Wrapper for Applying Atomic Functions to Arrays.
#'
#' This wrapper applies a given function, which takes N [multi-dimensional] arrays as inputs (which may have different numbers of dimensions and dimension lengths), and applies it to a list of N [multi-dimensional] arrays with at least as many dimensions as expected by the given function. The user can specify which dimensions of each array (or matrix) the function is to be applied over with the \code{margins} or \code{target_dims} option. A user can apply a function that receives (in addition to other helper parameters) 1 or more arrays as input, each with a different number of dimensions, and returns any number of multidimensional arrays. The target dimensions can be specified by their names. It is recommended to use this wrapper with multidimensional arrays with named dimensions.
#' @param data A single object (vector, matrix or array) or a list of objects. They must be in the same order as expected by AtomicFun.
#' @param target_dims List of vectors containing the dimensions to be input into AtomicFun for each of the objects in the data. These vectors can contain either integers specifying the dimension position, or characters corresponding to the dimension names. This parameter is mandatory if margins is not specified. If both margins and target_dims are specified, margins takes priority over target_dims.
#' @param AtomicFun Function to be applied to the arrays.
#' @param ... Additional arguments to be used in the AtomicFun.
#' @param output_dims Optional list of vectors containing the names of the dimensions to be output from the AtomicFun for each of the objects it returns (or a single vector if the function has only one output).
#' @param margins List of vectors containing the margins for the input objects to be split by. Or, if there is a single vector of margins specified and a list of objects in data, then the single set of margins is applied over all objects. These vectors can contain either integers specifying the dimension position, or characters corresponding to the dimension names. If both margins and target_dims are specified, margins takes priority over target_dims.
#' @param ncores The number of multicore threads to use for parallel computation.
#' @details When using a single object as input, Apply is almost identical to the apply function. For multiple input objects, the output array will have dimensions equal to the dimensions specified in 'margins'.
#' @return List of arrays or matrices or vectors resulting from applying AtomicFun to data.
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
#' test <- Apply(data, margins = margins, AtomicFun = "test_fun")
Apply <- function(data, target_dims = NULL, AtomicFun, ..., output_dims = NULL,
                  margins = NULL, ncores = NULL) {
  # Check data
  if (!is.list(data)) {
    data <- list(data)
  }
  if (any(!sapply(data, is.numeric))) {
    stop("Parameter 'data' must be one or a list of numeric objects.")
  }
  is_vector <- rep(FALSE, length(data))
  is_unnamed <- rep(FALSE, length(data))
  for (i in 1 : length(data)) {
    if (is.null(dim(data[[i]]))) {
      is_vector[i] <- TRUE
      is_unnamed[i] <- TRUE
      dim(data[[i]]) <- length(data[[i]])
    }
    if (!is.null(names(dim(data[[i]])))) {
      if (any(sapply(names(dim(data[[i]])), nchar) == 0)) {
        stop("Dimension names of arrays in 'data' must be at least ",
             "one character long.")
      }
    } else {
      is_unnamed[i] <- TRUE
    }
  }

  # Check AtomicFun
  if (is.character(AtomicFun)) {
    try({AtomicFun <- get(AtomicFun)}, silent = TRUE)
    if (!is.function(AtomicFun)) {
      stop("Could not find the function '", AtomicFun, "'.")
    }
  }
  if (!is.function(AtomicFun)) {
    stop("Parameter 'AtomicFun' must be a function or a character string ",
         "with the name of a function.")
  }
  if ('startR_step' %in% class(AtomicFun)) {
    if (is.null(target_dims)) {
      target_dims <- attr(AtomicFun, 'target_dims')
    }
    if (is.null(output_dims)) {
      output_dims <- attr(AtomicFun, 'output_dims')
    }
  }

  # Check target_dims and margins
  if (is.null(margins) && is.null(target_dims)) {
    stop("One of 'margins' or 'target_dims' must be specified.")
  }
  if (!is.null(margins)) {
    target_dims <- NULL
  }
  margins_names <- vector('list', length(data))
  target_dims_names <- vector('list', length(data))
  if (!is.null(margins)) {
  # Check margins and build target_dims accordingly
    if (!is.list(margins)) {
      margins <- rep(list(margins), length(data))
    }
    if (any(!sapply(margins, 
                    function(x) is.character(x) || is.numeric(x) || is.null(x)))) {
      stop("Parameter 'margins' must be one or a list of numeric or ",
           "character vectors.")
    }
    duplicate_dim_specs <- sapply(margins, 
      function(x) {
        length(unique(x)) != length(x)
      })
    if (any(duplicate_dim_specs)) {
      stop("Parameter 'margins' must not contain duplicated dimension ",
           "specifications.")
    }
    target_dims <- vector('list', length(data))
    for (i in 1 : length(data)) {
      if (length(margins[[i]]) > 0) {
        if (is.character(unlist(margins[i]))) {
          if (is.null(names(dim(data[[i]])))) {
            stop("Parameter 'margins' contains dimension names, but ",
                 "some of the corresponding objects in 'data' do not have ",
                 "dimension names.")
          }
          margins2 <- margins[[i]]
          margins2_new_num <- c()
          for (j in 1 : length(margins2)) {
            matches <- which(names(dim(data[[i]])) == margins2[j])
            if (length(matches) < 1) {
              stop("Could not find dimension '", margins2[j], "' in ", i, 
                   "th object provided in 'data'.")
            }
            margins2_new_num[j] <- matches[1]
          }
          margins_names[[i]] <- margins[[i]]
          margins[[i]] <- margins2_new_num
        }
        if (!is.null(names(dim(data[[i]])))) {
          target_dims_names[[i]] <- names(dim(data[[i]]))[- margins[[i]]]
        }
        target_dims[[i]] <- (1 : length(dim(data[[i]])))[- margins[[i]]]
      } else {
        target_dims[[i]] <- 1 : length(dim(data[[i]]))
        if (!is.null(names(dim(data[[i]])))) {
          target_dims_names[[i]] <- names(dim(data[[i]]))
        }
      }
    }
  } else {
  # Check target_dims and build margins accordingly
    if (!is.list(target_dims)) {
      target_dims <- rep(list(target_dims), length(data))
    }
    if (any(!sapply(target_dims, 
                    function(x) is.character(x) || is.numeric(x)))) {
      stop("Parameter 'target_dims' must be one or a list of numeric or ",
           "character vectors.")
    }
    if (any(sapply(target_dims, length) == 0)) {
      stop("Parameter 'target_dims' must not contain length-0 vectors.")
    }
    duplicate_dim_specs <- sapply(target_dims, 
      function(x) {
        length(unique(x)) != length(x)
      })
    if (any(duplicate_dim_specs)) {
      stop("Parameter 'target_dims' must not contain duplicated dimension ",
           "specifications.")
    }
    margins <- vector('list', length(data))
    for (i in 1 : length(data)) {
      if (is.character(unlist(target_dims[i]))) {
        if (is.null(names(dim(data[[i]])))) {
          stop("Parameter 'target_dims' contains dimension names, but ",
               "some of the corresponding objects in 'data' do not have ",
               "dimension names.")
        }
        targs2 <- target_dims[[i]]
        targs2_new_num <- c()
        for (j in 1 : length(targs2)) {
          matches <- which(names(dim(data[[i]])) == targs2[j])
          if (length(matches) < 1) {
            stop("Could not find dimension '", targs2[j], "' in ", i, 
                 "th object provided in 'data'.")
          }
          targs2_new_num[j] <- matches[1]
        }
        target_dims_names[[i]] <- target_dims[[i]]
        target_dims[[i]] <- targs2_new_num
      }
      if (!is.null(names(dim(data[[i]])))) {
        margins_names[[i]] <- names(dim(data[[i]]))[- target_dims[[i]]]
      }
      margins[[i]] <- (1 : length(dim(data[[i]])))[- target_dims[[i]]]
    }
  }
  # Reorder dimensions of input data for target dims to be left-most
  # and in the required order.
  for (i in 1 : length(data)) {
    if (length(target_dims[[i]]) > 0) {
      if (is.unsorted(target_dims[[i]]) || 
          (max(target_dims[[i]]) > length(target_dims[[i]]))) {
        marg_dims <- (1 : length(dim(data[[i]])))[- target_dims[[i]]]
        data[[i]] <- .aperm2(data[[i]], c(target_dims[[i]], marg_dims))
        target_dims[[i]] <- 1 : length(target_dims[[i]])
        margins[[i]] <- (length(target_dims[[i]]) + 1) : length(dim(data[[i]]))
      }
    }
  }

  # Check output_dims
  if (!is.null(output_dims)) {
    if (!is.list(output_dims)) {
      output_dims <- list(output1 = output_dims)
    }
    if (any(sapply(output_dims, function(x) !(is.character(x) || is.null(x))))) {
      stop("Parameter 'output_dims' must be one or a list of vectors of character strings (or NULLs).")
    }
    if (is.null(names(output_dims))) {
      names(output_dims) <- rep('', length(output_dims))
    }
    missing_output_names <- which(sapply(names(output_dims), nchar) == 0)
    if (length(missing_output_names) > 0) {
      names(output_dims)[missing_output_names] <- paste0('output', missing_output_names)
    }
  }

  # Check ncores
  if (is.null(ncores)) {
    ncores <- 1
  }
  if (!is.numeric(ncores)) {
    stop("Parameter 'ncores' must be numeric.")
  }
  ncores <- round(ncores)

  # Consistency checks of margins of all input objects
  #  for each data array, add its margins to the list if not present.
  #    if there are unnamed margins in the list, check their size matches the margins being added 
  #                                              and simply assing them a name
  #    those margins present, check that they match
  #      if unnamed margins, check consistency with found margins
  #        if more mrgins than found, add numbers to the list, without names
  #  with this we end up with a named list of margin sizes
  #  for data arrays with unnamed margins, we can assume their margins names are those of the first entries in the resulting list
  all_found_margins_lengths <- afml <- list()
  for (i in 1:length(data)) {
    if (!is.null(margins_names[[i]])) {
      if (length(afml) > 0) {
        matches <- which(margins_names[[i]] %in% names(afml))
        if (length(matches) > 0) {
          margs_to_add <- as.list(dim(data[[i]])[margins[[i]]][- matches])
          if (any(dim(data[[i]])[margins[[i]][matches]] != unlist(afml[margins_names[[i]][matches]]))) {
            stop("Found one or more margin dimensions with the same name and ",
                 "different length in some of the input objects in 'data'.")
          }
        } else {
          margs_to_add <- as.list(dim(data[[i]])[margins[[i]]])
        }
        unnamed_margins <- which(sapply(names(afml), nchar) == 0)
        if (length(unnamed_margins) > 0) {
          stop_with_error <- FALSE
          if (length(unnamed_margins) <= length(margs_to_add)) {
            if (any(unlist(afml[unnamed_margins]) != unlist(margs_to_add[1:length(unnamed_margins)]))) {
              stop_with_error <- TRUE
            }
            names(afml)[unnamed_margins] <- names(margs_to_add)[1:length(unnamed_margins)]
            margs_to_add <- margs_to_add[- (1:length(margs_to_add))]
          } else {
            if (any(unlist(afml[unnamed_margins[1:length(margs_to_add)]]) != unlist(margs_to_add))) {
              stop_with_error <- TRUE
            }
            names(afml)[unnamed_margins[1:length(margs_to_add)]] <- names(margs_to_add)
            margs_to_add <- list()
          }
          if (stop_with_error) {
            stop("Found unnamed margins (for some objects in parameter ",
                 "'data') that have been associated by their position to ",
                 "named margins in other objects in 'data' and do not have ",
                 "matching length. It could also be that the unnamed ",
                 "margins don not follow the same order as the named ",
                 "margins. In that case, either put the corresponding names ",
                 "to the dimensions of the objects in 'data', or put them ",
                 "in a consistent order.")
          }
        }
        afml <- c(afml, margs_to_add)
      } else {
        afml <- as.list(dim(data[[i]])[margins[[i]]])
      }
    } else {
      margs_to_add <- as.list(dim(data[[i]])[margins[[i]]])
      names(margs_to_add) <- rep('', length(margs_to_add))
      if (length(afml) > 0) {
        stop_with_error <- FALSE
        if (length(afml) >= length(margs_to_add)) {
          if (any(unlist(margs_to_add) != unlist(afml[1:length(margs_to_add)]))) {
            stop_with_error <- TRUE
          }
        } else {
          if (any(unlist(margs_to_add)[1:length(afml)] != unlist(afml))) {
            stop_with_error <- TRUE
          }
          margs_to_add <- margs_to_add[- (1:length(afml))]
          afml <- c(afml, margs_to_add)
        }
        if (stop_with_error) {
          stop("Found unnamed margins (for some objects in parameter ",
               "'data') that have been associated by their position to ",
               "named margins in other objects in 'data' and do not have ",
               "matching length. It could also be that the unnamed ",
               "margins don not follow the same order as in other ",
               "objects. In that case, either put the corresponding names ",
               "to the dimensions of the objects in 'data', or put them ",
               "in a consistent order.")
        }
      } else {
        afml <- margs_to_add
      }
    }
  }
  missing_margin_names <- which(names(afml) == '')
  if (length(missing_margin_names) > 0) {
    names(afml)[missing_margin_names] <- paste0('_unnamed_margin_', 
                                                1:length(missing_margin_names), '_')
  }
  # afml is now a named list with the lenghts of all margins. Each margin 
  # appears once only. If some names are not provided, they are set automatically
  # to 'unnamed_dim_1', 'unamed_dim_2', ...

  # Now need to check which margins are common for all the data arrays. 
  # Those will be used by llply.
  # For the margins that are not common, we will need to iterate manually 
  # across them, and use data arrays repeatedly as needed.
  margins_afml <- margins
  for (i in 1:length(data)) {
    if (!is.null(margins_names[[i]])) {
      
      margins_afml[[i]] <- sapply(margins_names[[i]], 
        function(x) {
          sapply(x, 
            function(y) {
              which(names(afml) == y)
            }
          )
        }
      )
    } else if (length(margins_afml[[i]]) > 0) {
      margins_afml[[i]] <- margins_afml[[i]] - min(margins_afml[[i]]) + 1
      # The missing margin and dim names are filled in.
      margins_names[[i]] <- names(afml)[margins_afml[[i]]]
      names(dim(data[[i]]))[margins[[i]]] <- margins_names[[i]]
    }
  }
  common_margs <- margins_afml[[1]]
  if (length(margins_afml) > 1) {
    for (i in 2:length(margins_afml)) {
      margs_a <- unlist(afml[common_margs])
      margs_b <- unlist(afml[margins_afml[[i]]])
      matches <- which(names(margs_a) %in% names(margs_b))
      if (length(matches) > 0) {
        common_margs <- common_margs[matches]
      } else {
        common_margs <- NULL
      }
    }
  }
  non_common_margs <- 1:length(afml)
  if (length(common_margs) > 0) {
    non_common_margs <- non_common_margs[- common_margs]
  }
  # common_margs is now a numeric vector with the indices of the common 
  # margins (i.e. their position in afml)
  # non_common_margs is now a numeric vector with the indices of the 
  # non-common margins (i.e. their position in afml)

  .isolate <- function(data, margin_length, drop = FALSE) {
    eval(dim(environment()$data))
    structure(list(env = environment(), index = margin_length, 
                   drop = drop, subs = as.name("[")), 
              class = c("indexed_array"))
  }
  .consolidate <- function(subsets, dimnames, out_dims) {
    lapply(setNames(1:length(subsets), names(subsets)), 
      function(x) {
        if (length(out_dims[[x]]) > 0) {
          dims <- dim(subsets[[x]])
          if (!is_unnamed[x]) {
            names(dims) <- dimnames[[x]]
          }
          dims <- dims[out_dims[[x]]]
          array(subsets[[x]], dim = dims)
        } else {
          as.vector(subsets[[x]])
        }
      })
  }

  data_indexed <- vector('list', length(data))
  data_indexed_indices <- vector('list', length(data))
  for (i in 1 : length(data)) {
    margs_i <- which(names(dim(data[[i]])) %in% names(afml[c(non_common_margs, common_margs)]))
    false_margs_i <- which(margs_i %in% target_dims[[i]])
    margs_i <- setdiff(margs_i, false_margs_i)
    if (length(margs_i) > 0) {
      margin_length <- lapply(dim(data[[i]]), function(x) 1 : x)
      margin_length[- margs_i] <- ""
    } else {
      margin_length <- as.list(rep("", length(dim(data[[i]]))))
    }
    margin_length <- expand.grid(margin_length, KEEP.OUT.ATTRS = FALSE,
                                 stringsAsFactors = FALSE)
    data_indexed[[i]] <- .isolate(data[[i]], margin_length)
    if (length(margs_i) > 0) {
      data_indexed_indices[[i]] <- array(1:prod(dim(data[[i]])[margs_i]),
                                         dim = dim(data[[i]])[margs_i])
    } else {
      data_indexed_indices[[i]] <- array(1, dim = 1)
    }
  }

  splatted_f <- splat(AtomicFun)

  # Iterate along all non-common margins
  if (length(c(non_common_margs, common_margs)) > 0) {
    marg_inds_ordered <- sort(c(non_common_margs, common_margs))
    margins_array <- ma <- array(1:prod(unlist(afml[marg_inds_ordered])), 
                                 dim = unlist(afml[marg_inds_ordered]))
  } else {
    ma <- array(1)
  }
  arrays_of_results <- NULL
  found_first_result <- FALSE

  total_size <- prod(dim(ma))
  if (!is.null(ncores)) {
    chunk_size <- round(total_size / (ncores * 4))
  } else {
    chunk_size <- 4
  }
  if (chunk_size < 1) {
    chunk_size <- 1
  }
  nchunks <- floor(total_size / chunk_size)
  chunk_sizes <- rep(chunk_size, nchunks)
  if (total_size %% chunk_size != 0) {
    chunk_sizes <- c(chunk_sizes, total_size %% chunk_size)
  }

# need to add progress bar
  iteration <- function(m) {
    sub_arrays_of_results <- list()
    found_first_sub_result <- FALSE
    for (n in 1:chunk_sizes[m]) {
      # j is the index of the data piece to load in data_indexed
      j <- n + (m - 1) * chunk_size
      marg_indices <- arrayInd(j, dim(ma))
      names(marg_indices) <- names(dim(ma))
      input <- list()
      # Each iteration of n, the variable input is populated with sub-arrays for 
      # each object in data (if possible). For each set of 'input's, the
      # splatted_f is applied in parallel if possible.
      for (i in 1:length(data_indexed)) {
        inds_to_take <- which(names(marg_indices) %in% names(dim(data_indexed_indices[[i]])))
        if (length(inds_to_take) > 0) {
          marg_inds_to_take <- marg_indices[inds_to_take][names(dim(data_indexed_indices[[i]]))]
          input[[i]] <- data_indexed[[i]][[do.call("[",
            c(list(x = data_indexed_indices[[i]]), marg_inds_to_take,
              list(drop = TRUE)))]]
        } else {
          input[[i]] <- data_indexed[[i]][[1]]
        }
      }
      result <- splatted_f(.consolidate(input, lapply(lapply(data, dim), names), 
                                        target_dims), ...)
      if (!is.list(result)) {
        result <- list(result)
      }
      if (!found_first_sub_result) {
        sub_arrays_of_results <- vector('list', length(result))
        if (!is.null(output_dims)) {
          if (length(output_dims) != length(sub_arrays_of_results)) {
            stop("The 'AtomicFun' returns ", length(sub_arrays_of_results), 
                 " elements, but ", length(output_dims), 
                 " elements were expected.")
          }
          names(sub_arrays_of_results) <- names(output_dims)
        } else if (!is.null(names(result))) {
          names(sub_arrays_of_results) <- names(result)
        } else {
          names(sub_arrays_of_results) <- paste0('output', 1:length(result))
        }
      }
      atomic_fun_out_dims <- vector('list', length(result))
      for (component in 1:length(result)) {
        if (is.null(dim(result[[component]]))) {
          if (length(result[[component]]) == 1) {
            component_dims <- NULL
          } else {
            component_dims <- length(result[[component]])
          }
        } else {
          component_dims <- dim(result[[component]])
        }
        if (!found_first_sub_result) {
          sub_arrays_of_results[[component]] <- array(dim = c(component_dims, chunk_sizes[m]))
        }
        if (!is.null(component_dims)) {
          atomic_fun_out_dims[[component]] <- component_dims
        }
        sub_arrays_of_results[[component]][(1:prod(component_dims)) + 
          (n - 1) * prod(component_dims)] <- result[[component]]
      }
      if (!found_first_sub_result) {
        found_first_sub_result <- TRUE
      }
      if (!is.null(output_dims)) {
        # Check number of outputs.
        if (length(output_dims) != length(result)) {
          stop("Expected AtomicFun to return ", length(output_dims), " components, ",
               "but ", length(result), " found.")
        }
        # Check number of output dimensions is correct.
        for (component in 1:length(result)) {
          if (length(atomic_fun_out_dims[[component]]) != length(output_dims[[component]])) {
            stop("Expected ", component, "st returned element by 'AtomicFun' ",
                 "to have ", length(output_dims[[component]]), " dimensions, ", 
                 "but ", length(atomic_fun_out_dims[[component]]), " found.")
          }
        }
      }
    }
    sub_arrays_of_results
  }

  # Execute in parallel if needed
  parallel <- ncores > 1
  if (parallel) registerDoParallel(ncores)
  result <- llply(1:length(chunk_sizes), iteration, .parallel = parallel)
  if (parallel) registerDoSEQ()

  # Merge the results
  chunk_length <- NULL
  fun_out_dims <- vector('list', length(result[[1]]))
  for (m in 1:length(result)) {
    if (!found_first_result) {
      arrays_of_results <- vector('list', length(result[[1]]))
      if (!is.null(output_dims)) {
        if (length(output_dims) != length(arrays_of_results)) {
          stop("The 'AtomicFun' returns ", length(arrays_of_results), " elements, but ", 
               length(output_dims), " elements were expected.")
        }
        names(arrays_of_results) <- names(output_dims)
      } else if (!is.null(names(result[[1]]))) {
        names(arrays_of_results) <- names(result[[1]])
      } else {
        names(arrays_of_results) <- paste0('output', 1:length(result[[1]]))
      }
    }
    for (component in 1:length(result[[m]])) {
      component_dims <- dim(result[[m]][[component]])
      if (!found_first_result) {
        if (length(component_dims) > 1) {
          fun_out_dims[[component]] <- component_dims[- length(component_dims)]
        }
        arrays_of_results[[component]] <- array(dim = c(fun_out_dims[[component]], 
                                                        dim(ma)))
        dimnames_to_remove <- which(grepl('^_unnamed_margin_',
                                    names(dim(arrays_of_results[[component]]))))
        if (length(dimnames_to_remove) > 0) {
          names(dim(arrays_of_results[[component]]))[dimnames_to_remove] <- rep('', length(dimnames_to_remove))
        }
        if (all(names(dim(arrays_of_results[[component]])) == '')) {
          names(dim(arrays_of_results[[component]])) <- NULL
        }
        chunk_length <- prod(component_dims)
      }
      arrays_of_results[[component]][(1:prod(component_dims)) + 
        (m - 1) * chunk_length] <- result[[m]][[component]]
    }
    if (!found_first_result) {
      found_first_result <- TRUE
    }
    #if (!is.null(output_dims)) {
    #  # Check number of output dimensions is correct.
    #  for (component in 1:length(atomic_fun_out_dims)) {
    #    if (!is.null(names(fun_out_dims[[component]]))) {
    #      # check component_dims match names of output_dims[[component]], and reorder if needed
    #    }
    #  }
    #}
  }
  # Assign 'output_dims' as dimension names if possible
  if (!is.null(output_dims)) {
    for (component in 1:length(output_dims)) {
      if (length(output_dims[[component]]) > 0) {
        names(dim(arrays_of_results[[component]]))[1:length(output_dims[[component]])] <- output_dims[[component]]
      }
    }
  }

  # Return
  arrays_of_results
}
