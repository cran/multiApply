#' Apply Functions to Multiple Multidimensional Arrays or Vectors
#'
#' This function efficiently applies a given function, which takes N vectors or multi-dimensional arrays as inputs (which may have different numbers of dimensions and dimension lengths), and applies it to a list of N vectors or multi-dimensional arrays with at least as many dimensions as expected by the given function. The user can specify which dimensions of each array the function is to be applied over with the \code{margins} or \code{target_dims} parameters. The function to be applied can receive other helper parameters and return any number of vectors or multidimensional arrays. The target dimensions or margins can be specified by their names, as long as the inputs are provided with dimension names (recommended). This function can also use multi-core in a transparent way if requested via the \code{ncores} parameter.\cr\cr The following steps help to understand how \code{Apply} works:\cr\cr - The function receives N arrays with Dn dimensions each.\cr - The user specifies, for each of the arrays, which of its dimensions are 'target' dimensions (dimensions which the function provided in 'fun' operates with) and which are 'margins' (dimensions to be looped over).\cr - \code{Apply} will generate an array with as many dimensions as margins in all of the input arrays. If a margin is repeated across different inputs, it will appear only once in the resulting array.\cr - For each element of this resulting array, the function provided in the parameter'fun' is applied to the corresponding sub-arrays in 'data'.\cr - If the function returns a vector or a multidimensional array, the additional dimensions will be prepended to the resulting array (in left-most positions).\cr - If the provided function returns more than one vector or array, the process above is carried out for each of the outputs, resulting in a list with multiple arrays, each with the combination of all target dimensions (at the right-most positions) and resulting dimensions (at the left-most positions).
#'
#' @param data One or a list of vectors, matrices or arrays. They must be in the same order as expected by the function provided in the parameter 'fun'. The dimensions do not necessarily have to be ordered. If the 'target_dims' require a different order than the provided, \code{Apply} will automatically reorder the dimensions as needed.
#' @param target_dims One or a list of vectors (or NULLs) containing the dimensions to be input into fun for each of the objects in the data. If a single vector of target dimensions is specified and multiple inputs are provided in 'data, then the single set of target dimensions is re-used for all of the inputs. These vectors can contain either integers specifying the position of the dimensions, or character strings corresponding to the dimension names. This parameter is mandatory if 'margins' are not specified. If both 'margins' and 'target_dims' are specified, 'margins' takes priority.
#' @param fun Function to be applied to the arrays. Must receive as many inputs as provided in 'data', each with as many dimensions as specified in 'target_dims' or as the total number of dimensions in 'data' minus the ones specified in 'margins'. The function can receive other additional fixed parameters (see parameter '...' of \code{Apply}). The function can return one or a list of vectors or multidimensional arrays, optionally with dimension names which will be propagated to the final result. The returned list can optionally be named, with a name for each output, which will be propagated to the resulting array. The function can optionally be provided with the attributes 'target_dims' and 'output_dims'. In that case, the corresponding parameters of \code{Apply} do not need to be provided. The function can expect named dimensions for each of its inputs, in the same order as specified in 'target_dims' or, if no 'target_dims' have been provided, in the same order as provided in 'data'. The function can access the variable \code{.margin_indices}, a named numeric vector that provides the indices of the current iteration over the margins, as well as any other variables specified in the parameter \code{extra_info} or input attributes specified in the parameter \code{use_attributes}.
#' @param ... Additional fixed arguments expected by the function provided in the parameter 'fun'.
#' @param output_dims Optional list of vectors containing the names of the dimensions to be output from the fun for each of the objects it returns (or a single vector if the function has only one output).
#' @param margins One or a list of vectors (or NULLs) containing the 'margin' dimensions to be looped over for each input in 'data'. If a single vector of margins is specified and multiple inputs are provided in 'data', then the single set of margins is re-used for all of the inputs. These vectors can contain either integers specifying the position of the margins, or character strings corresponding to the dimension names. If both 'margins' and 'target_dims' are specified, 'margins' takes priority.
#' @param use_attributes List of vectors of character strings with names of attributes of each object in 'data' to be propagated to the subsets of data sent as inputs to the function specified in 'fun'. If this parameter is not specified (NULL), all attributes are dropped. This parameter can be specified as a named list (then the names of this list must match those of the names of parameter 'data'), or as an unnamed list (then the vectors of attribute names will be assigned in order to the input arrays in 'data').
#' @param extra_info Named list of extra variables to be defined for them to be accessible from within the function specified in 'fun'. The variable names will automatically be prepended a heading dot ('.'). So, if the variable 'name = "Tony"' is sent through this parameter, it will be accessible from within 'fun' via '.name'.
#' @param guess_dim_names Whether to automatically guess missing dimension names for dimensions of equal length across different inputs in 'data' with a warning (TRUE; default), or to crash whenever unnamed dimensions of equa length are identified across different inputs (FALSE).
#' @param ncores The number of parallel processes to spawn for the use for parallel computation in multiple cores.
#' @param split_factor Factor telling to which degree the input data should be split into smaller pieces to be processed by the available cores. By default (split_factor = 1) the data is split into 4 pieces for each of the cores (as specified in ncores). A split_factor of 2 will result in 8 pieces for each of the cores, and so on. The special value 'greatest' will split the input data into as many pieces as possible.
#' @details When using a single object as input, Apply is almost identical to the apply function (as fast or slightly slower in some cases; with equal or improved -smaller- memory footprint).
#' @return List of arrays or matrices or vectors resulting from applying 'fun' to 'data'.
#' @references Wickham, H (2011), The Split-Apply-Combine Strategy for Data Analysis, Journal of Statistical Software.
#' @export
#' @examples
#' #Change in the rate of exceedance for two arrays, with different 
#' #dimensions, for some matrix of exceedances.
#' data <- list(array(rnorm(1000), c(5, 10, 20)), 
#'              array(rnorm(500), c(5, 10, 10)), 
#'              array(rnorm(50), c(5, 10)))
#' test_fun <- function(x, y, z) {
#'   ((sum(x > z) / (length(x))) / 
#'   (sum(y > z) / (length(y)))) * 100
#' }
#' test <- Apply(data, target = list(3, 3, NULL), test_fun)
#' @importFrom foreach registerDoSEQ
#' @importFrom doParallel registerDoParallel
#' @importFrom plyr splat llply
#' @importFrom utils capture.output
#' @importFrom stats setNames
Apply <- function(data, target_dims = NULL, fun, ..., 
                  output_dims = NULL, margins = NULL, 
                  use_attributes = NULL, extra_info = NULL,
                  guess_dim_names = TRUE,
                  ncores = NULL, split_factor = 1) {
  # Check data
  if (!is.list(data)) {
    data <- list(data)
  }
  #if (any(!sapply(data, is.numeric))) {
  #  stop("Parameter 'data' must be one or a list of numeric objects.")
  #}
  is_vector <- rep(FALSE, length(data))
  is_unnamed <- rep(FALSE, length(data))
  unnamed_dims <- c()
  guessed_any_dimnames <- FALSE
  for (i in 1 : length(data)) {
    if (length(data[[i]]) < 1) {
      stop("Arrays in 'data' must be of length > 0.")
    }
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
      if (length(unique(names(dim(data[[i]])))) != length(names(dim(data[[i]])))) {
        stop("Arrays in 'data' must not have repeated dimension names.")
      }
      if (any(is.na(names(dim(data[[i]]))))) {
        stop("Arrays in 'data' must not have NA as dimension names.")
      } 
    } else {
      is_unnamed[i] <- TRUE
      new_unnamed_dims <- c()
      unnamed_dims_copy <- unnamed_dims
      for (j in 1 : length(dim(data[[i]]))) {
        len_of_dim_j <- dim(data[[i]])[j]
        found_match <- which(unnamed_dims_copy == len_of_dim_j)
        if (!guess_dim_names && (length(found_match) > 0)) {
          stop("Arrays in 'data' have multiple unnamed dimensions of the ",
               "same length. Please provide dimension names.")
        }
        if (length(found_match) > 0) {
          found_match <- found_match[1]
          names(dim(data[[i]]))[j] <- names(unnamed_dims_copy[found_match])
          unnamed_dims_copy <- unnamed_dims_copy[-found_match]
          guessed_any_dimnames <- TRUE
        } else {
          new_dim <- len_of_dim_j
          names(new_dim) <- paste0('_unnamed_dim_', length(unnamed_dims) + 
                                   length(new_unnamed_dims) + 1, '_')
          new_unnamed_dims <- c(new_unnamed_dims, new_dim)
          names(dim(data[[i]]))[j] <- names(new_dim)
        }
      }
      unnamed_dims <- c(unnamed_dims, new_unnamed_dims)
    }
  }
  if (guessed_any_dimnames) {
    dim_names_string <- ""
    for (i in 1:length(data)) {
      dim_names_string <- c(dim_names_string, "\n\tInput ", i, ":",
        sapply(capture.output(print(dim(data[[i]]))), 
               function(x) paste0('\n\t\t', x)))
    }
    warning("Guessed names for some unnamed dimensions of equal length ",
            "found across different inputs in 'data'. Please check ",
            "carefully the assumed names below are correct, or provide ",
            "dimension names for safety, or disable the parameter ",
            "'guess_dim_names'.", dim_names_string)
  }

  # Check fun
  if (is.character(fun)) {
    fun_name <- fun
    err <- try({
      fun <- get(fun)
    }, silent = TRUE)
    if (!is.function(fun)) {
      stop("Could not find the function '", fun_name, "'.")
    }
  }
  if (!is.function(fun)) {
    stop("Parameter 'fun' must be a function or a character string ",
         "with the name of a function.")
  }
  if (!is.null(attributes(fun))) {
    if (is.null(target_dims)) {
      if ('target_dims' %in% names(attributes(fun))) {
        target_dims <- attr(fun, 'target_dims')
      }
    }
    if (is.null(output_dims)) {
      if ('output_dims' %in% names(attributes(fun))) {
        output_dims <- attr(fun, 'output_dims')
      }
    }
  }

  # Check target_dims and margins
  arglist <- as.list(match.call())
  if (!any(c('margins', 'target_dims') %in% names(arglist)) &&
      is.null(target_dims)) {
    stop("One of 'margins' or 'target_dims' must be specified.")
  }
  margins_names <- vector('list', length(data))
  target_dims_names <- vector('list', length(data))
  if ('margins' %in% names(arglist)) {
  # Check margins and build target_dims accordingly
    if (!is.list(margins)) {
      margins <- rep(list(margins), length(data))
    }
    if (any(!sapply(margins, 
                    function(x) is.character(x) || is.numeric(x) || is.null(x)))) {
      stop("Parameter 'margins' must be one or a list of numeric or ",
           "character vectors.")
    }
    if (any(sapply(margins, function(x) is.character(x) && (length(x) == 0)))) {
      stop("Parameter 'margins' must not contain length-0 character vectors.")
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
        if (length(margins[[i]]) == length(dim(data[[i]]))) {
          target_dims_names[i] <- list(NULL)
          target_dims[i] <- list(NULL)
          margins_names[[i]] <- names(dim(data[[i]]))
        } else {
          margins_names[[i]] <- names(dim(data[[i]]))[margins[[i]]]
          target_dims_names[[i]] <- names(dim(data[[i]]))[- margins[[i]]]
          target_dims[[i]] <- (1 : length(dim(data[[i]])))[- margins[[i]]]
        }
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
                    function(x) is.character(x) || is.numeric(x) || is.null(x)))) {
      stop("Parameter 'target_dims' must be one or a list of numeric or ",
           "character vectors.")
    }
    if (any(sapply(target_dims, function(x) is.character(x) && (length(x) == 0)))) {
      stop("Parameter 'target_dims' must not contain length-0 character vectors.")
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
      if (length(target_dims[[i]]) > 0) {
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
        if (length(target_dims[[i]]) == length(dim(data[[i]]))) {
          margins_names[i] <- list(NULL)
          margins[i] <- list(NULL)
          target_dims_names[[i]] <- names(dim(data[[i]]))
        } else {
          target_dims_names[[i]] <- names(dim(data[[i]]))[target_dims[[i]]]
          margins_names[[i]] <- names(dim(data[[i]]))[- target_dims[[i]]]
          margins[[i]] <- (1 : length(dim(data[[i]])))[- target_dims[[i]]]
        }
      } else {
        margins[[i]] <- 1 : length(dim(data[[i]]))
        if (!is.null(names(dim(data[[i]])))) {
          margins_names[[i]] <- names(dim(data[[i]]))
        }
      }
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
        target_dims_names[[i]] <- names(dim(data[[i]]))[target_dims[[i]]]
        if (length(target_dims[[i]]) < length(dim(data[[i]]))) {
          margins[[i]] <- (length(target_dims[[i]]) + 1) : length(dim(data[[i]]))
          margins_names[[i]] <- names(dim(data[[i]]))[margins[[i]]]
        }
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

  # Check use_attributes
  if (!is.null(use_attributes)) {
    if (!is.list(use_attributes)) {
      stop("Parameter 'use_attributes' must be a list.")
    }
    if (is.null(names(data)) && !is.null(names(use_attributes))) {
      warning("Parameter 'use_attributes' provided with names, but ",
              "no names provided for 'data'. All names will be ",
              "disregarded.")
      names(use_attributes) <- NULL
    }
    if (!is.null(names(use_attributes))) {
      if (!all(sapply(names(use_attributes), function(x) nchar(x) > 0))) {
        stop("If providing names for the list 'use_attributes', all ",
             "components must be named.")
      }
      if (length(unique(names(use_attributes))) != 
          length(names(use_attributes))) {
        stop("The list in parameter 'use_attributes' must not ",
             "contain repeated names.")
      }
      if (any(!(names(use_attributes) %in% names(data)))) {
        stop("Provided some names in parameter 'use_attributes' not present ",
             "in parameter 'data'.")
      }
      use_attributes <- use_attributes[names(data)]
    } else {
      if (length(use_attributes) != length(data)) {
        warning("Provided different number of items in 'use_attributes' ",
                "and in 'data'. Assuming same order.")
      }
      use_attributes <- use_attributes[1:length(data)]
    }
  } else {
    use_attributes <- vector('list', length = length(data))
  }
  for (i in 1:length(data)) {
    if (is.character(use_attributes[[i]])) {
      use_attributes[[i]] <- as.list(use_attributes[[i]])
    }
    if (is.list(use_attributes[[i]])) {
      if (length(use_attributes[[i]]) == 0) {
        use_attributes[i] <- list(NULL)
      } else {
        if (!all(sapply(use_attributes[[i]], 
              function(x) all(is.character(x) & nchar(x) > 0)))) {
          stop("All entries in 'use_attributes' must be character strings ",
               "of length > 0.")
        }
      }
    } else if (!is.null(use_attributes[[i]])) {
      stop("Parameter 'use_attributes' must be a list of character vectors or ",
           "a list of lists of character vectors.")
    }
    for (j in seq_along(use_attributes[[i]])) {
      if (length(use_attributes[[i]][[j]]) == 1 && 
          use_attributes[[i]][[j]] == 'dim') {
        stop("Requesting the attribute 'dim' via the parameter ",
             "'use_attributes' is forbidden.")
      }
      found_entry <- FALSE
      entry <- try({`[[`(attributes(data[[i]]), 
                         use_attributes[[i]][[j]])}, silent = TRUE)
      if ('try-error' %in% class(entry)) {
        stop("Parameter 'use_attributes' contains some attribute names ",
             "that are not present in the attributes of the corresponding ",
             "object in parameter 'data'.")
      }
    }
  }

  # Check extra_info
  if (is.null(extra_info)) {
    extra_info <- list()
  }
  raise_error <- FALSE
  if (!is.list(extra_info)) {
    raise_error <- TRUE
  } else if (length(extra_info) > 0) {
    if (is.null(names(extra_info))) {
      raise_error <- TRUE
    }
    if (any(sapply(names(extra_info), function(x) nchar(x) == 0))) {
      raise_error <- TRUE
    }
    names(extra_info) <- paste0('.', names(extra_info))
  }
  if (raise_error) {
    stop("Parameter 'extra_info' must be a list with all components named.")
  }

  # Check guess_dim_names
  if (!is.logical(guess_dim_names)) {
    stop("Parameter 'guess_dim_names' must be logical.")
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
  #    those margins present, check that they match
  #  with this we end up with a named list of margin sizes
  all_found_margins_lengths <- afml <- list()
  for (i in 1:length(data)) {
    #if (!is.null(margins_names[[i]])) {
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
        afml <- c(afml, margs_to_add)
      } else {
        afml <- as.list(dim(data[[i]])[margins[[i]]])
      }
    #}
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
    if (length(margins[[i]]) > 0) { 
      margins_afml[[i]] <- sapply(margins_names[[i]], 
        function(x) {
          sapply(x, 
            function(y) {
              which(names(afml) == y)
            }
          )
        }
      )
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
  if (length(afml) > 0) {
    non_common_margs <- 1:length(afml)
    if (length(common_margs) > 0) {
      non_common_margs <- non_common_margs[- common_margs]
    }
  } else {
    non_common_margs <- NULL
  }
  # common_margs is now a numeric vector with the indices of the common 
  # margins (i.e. their position in afml)
  # non_common_margs is now a numeric vector with the indices of the 
  # non-common margins (i.e. their position in afml)
  if (length(c(non_common_margs, common_margs)) > 0) {
    marg_inds_ordered <- sort(c(non_common_margs, common_margs))
    margins_array_dims <- mad <- unlist(afml[marg_inds_ordered])
  } else {
    margins_array_dims <- mad <- NULL
  }

  # Sharing workload across cores. Each core will run 4 chunks if possible.
  # the larger the split factor, the smaller the amount of data that 
  # will be processed at once and the finer the granules to be distributed
  # across cores, but the larger the overhead for granule startup, etc.
  total_size <- prod(mad)
  if (split_factor == 'greatest') {
    chunks_per_core <- ceiling(total_size / ncores)
  } else {
    chunks_per_core <- 4 * split_factor
  }
  if (!is.null(ncores)) {
    chunk_size <- round(total_size / (ncores * chunks_per_core))
  }
  #} else {
  #  chunk_size <- 4
  #}
  if (chunk_size < 1) {
    chunk_size <- 1
  }
  nchunks <- floor(total_size / chunk_size)
  chunk_sizes <- rep(chunk_size, nchunks)
  if (total_size %% chunk_size != 0) {
    chunk_sizes <- c(chunk_sizes, total_size %% chunk_size)
  }

  fun_env <- new.env(parent = environment(fun))
  for (i in seq_along(extra_info)) {
    assign(names(extra_info)[i], extra_info[[i]], envir = fun_env)
  }
  environment(fun) <- fun_env
  splatted_f <- splat(fun)

  input_margin_weights <- vector('list', length(data))
  for (i in 1:length(data)) {
    marg_sizes <- dim(data[[i]])[margins[[i]]]
    input_margin_weights[[i]] <- sapply(1:length(marg_sizes),
      function(k) prod(c(1, marg_sizes)[1:k]))
  }

  # TODO: need to add progress bar
  # For a selected use case, these are the timings:
  #  - total: 17 s
  #    - preparation + post: 1 s
  #    - llply (40 iterations): 16 s
  #      - one iteration: 1.5s with profiling of 50 sub-iterations (0.4 without)
  #        - intro: 0 s
  #        - for loop with profiling of 50 sub-iterations (5000 sub-iterations): 1.5 s
  #          - one sub-iteration: 0.0003 s
  #            - intro: 0.000125 s
  #            - splatted_f: 0.000125 s
  #            - outro: 0.00005 
  iteration <- function(m) {
    # INTRO
    n <- 1
    first_index <- n + (m - 1) * chunk_size
    first_marg_indices <- arrayInd(first_index, mad)
    names(first_marg_indices) <- names(mad)
    sub_arrays_of_results <- list()
    found_first_sub_result <- FALSE
    attributes_to_send <- vector('list', length = length(data))
    iteration_indices_to_take <- list()
    for (i in 1:length(data)) {
      iteration_indices_to_take[[i]] <- as.list(rep(TRUE, length(dim(data[[i]]))))
      names(iteration_indices_to_take[[i]]) <- names(dim(data[[i]]))
      if (length(use_attributes[[i]]) > 0) {
        attributes_to_send[[i]] <- list()
        for (j in seq_along(use_attributes[[i]])) {
          found_entry <- FALSE
          entry <- try({`[[`(attributes(data[[i]]), 
                             use_attributes[[i]][[j]])
                       }, silent = TRUE)
          if ('try-error' %in% class(entry)) {
            stop("Unexpected error with the attributes of the inputs.") 
          }
          save_string <- "attributes_to_send[[i]]"
          access_string <- "`[[`(attributes(data[[i]]), use_attributes[[i]][[j]])"
          for (k in seq_along(use_attributes[[i]][[j]])) {
            save_string <- paste0(save_string, '$', use_attributes[[i]][[j]][[k]])
          }
          eval(parse(text = paste(save_string, '<-', access_string)))
        }
      }
    }

    add_one_multidim <- function(index, dims) {
      stop_iterating <- FALSE
      check_dim <- 1
      ndims <- length(index)
      while (!stop_iterating) {
        index[check_dim] <- index[check_dim] + 1
        if (index[check_dim] > dims[check_dim]) {
          index[check_dim] <- 1
          check_dim <- check_dim + 1
          if (check_dim > ndims) {
            check_dim <- rep(1, ndims)
            stop_iterating <- TRUE
          }
        } else {
          stop_iterating <- TRUE
        }
      }
      index
    }

    # FOR LOOP
    for (n in 1:chunk_sizes[m]) {
      # SUB-ITERATION INTRO
      iteration_input <- list()
      for (i in 1:length(data)) {
        input_margin_dim_index <- first_marg_indices[margins_names[[i]]]
        iteration_indices_to_take[[i]][margins_names[[i]]] <- input_margin_dim_index
        iteration_input[[i]] <- do.call('[', c(list(x = data[[i]]),
                                               iteration_indices_to_take[[i]],
                                               list(drop = FALSE)))
        num_targets <- length(target_dims_names[[i]])
        if (num_targets > 0) {
          names(dim(iteration_input[[i]])) <- names(dim(data[[i]]))
        }
        num_margins <- length(margins_names[[i]])
        if (num_margins > 0) {
          if (num_margins == length(dim(iteration_input[[i]]))) {
            dim(iteration_input[[i]]) <- NULL
          } else {
            dims_to_remove <- 1:num_margins + length(target_dims[[i]])
            dim(iteration_input[[i]]) <- dim(iteration_input[[i]])[-dims_to_remove]
            #if only one dim remains, make as.vector
          }
        }
        attributes(iteration_input[[i]]) <- c(attributes(iteration_input[[i]]),
                                              attributes_to_send[[i]])
      }

      assign(".margin_indices", 
             setNames(as.integer(first_marg_indices),
                      names(first_marg_indices)), 
             envir = fun_env)

      # SPLATTED_F
      result <- splatted_f(iteration_input, ...)

      if (!is.null(mad)) {
        first_marg_indices <- add_one_multidim(first_marg_indices, mad)
      }

      # SUB-ITERATION OUTRO
      if (!is.list(result)) {
        result <- list(result)
      }
      if (!found_first_sub_result) {
        sub_arrays_of_results <- vector('list', length(result))
        if (!is.null(output_dims)) {
          if (length(output_dims) != length(sub_arrays_of_results)) {
            stop("The 'fun' returns ", length(sub_arrays_of_results), 
                 " elements, but ", length(output_dims), 
                 " elements were expected.")
          }
          names(sub_arrays_of_results) <- names(output_dims)
        } else if (!is.null(names(result))) {
          names(sub_arrays_of_results) <- names(result)
        } else {
          names(sub_arrays_of_results) <- paste0('output', 1:length(result))
        }
        len0_names <- which(nchar(names(sub_arrays_of_results)) == 0)
        if (length(len0_names) > 0) {
          names(sub_arrays_of_results)[len0_names] <- paste0('output', len0_names)
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
        if (length(result[[component]]) > 0) {
          sub_arrays_of_results[[component]][(1:prod(component_dims)) + 
            (n - 1) * prod(component_dims)] <- result[[component]]
        }
      }
      if (!found_first_sub_result) {
        found_first_sub_result <- TRUE
      }
      if (!is.null(output_dims)) {
        # Check number of outputs.
        if (length(output_dims) != length(result)) {
          stop("Expected fun to return ", length(output_dims), " components, ",
               "but ", length(result), " found.")
        }
        # Check number of output dimensions is correct.
        for (component in 1:length(result)) {
          if (length(atomic_fun_out_dims[[component]]) != length(output_dims[[component]])) {
            stop("Expected ", component, "st returned element by 'fun' ",
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
  arrays_of_results <- NULL
  found_first_result <- FALSE
  result_chunk_lengths <- vector('list', length(result[[1]]))
  fun_out_dims <- vector('list', length(result[[1]]))
  for (m in 1:length(result)) {
    if (!found_first_result) {
      arrays_of_results <- vector('list', length(result[[1]]))
      if (!is.null(output_dims)) {
        if (length(output_dims) != length(arrays_of_results)) {
          stop("The 'fun' returns ", length(arrays_of_results), " elements, but ", 
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
        result_chunk_lengths[[component]] <- prod(component_dims)
        if (length(component_dims) > 1) {
          fun_out_dims[[component]] <- component_dims[- length(component_dims)]
        }
        if (length(fun_out_dims[[component]]) + length(mad) > 0) {
          arrays_of_results[[component]] <- array(dim = c(fun_out_dims[[component]], 
                                                          mad))
          dimnames_to_remove <- which(grepl('^_unnamed_dim_',
                                      names(dim(arrays_of_results[[component]]))))
          if (length(dimnames_to_remove) > 0) {
            names(dim(arrays_of_results[[component]]))[dimnames_to_remove] <- rep('', length(dimnames_to_remove))
          }
          if (all(names(dim(arrays_of_results[[component]])) == '')) {
            names(dim(arrays_of_results[[component]])) <- NULL
          }
        }
      }
      arrays_of_results[[component]][(1:prod(component_dims)) + 
        (m - 1) * result_chunk_lengths[[component]]] <- result[[m]][[component]]
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
