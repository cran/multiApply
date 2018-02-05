# Function to permute arrays of non-atomic elements (e.g. POSIXct)
.aperm2 <- function(x, new_order) {
  y <- array(1:length(x), dim = dim(x))
  y <- aperm(y, new_order)
  old_dims <- dim(x)
  x <- x[as.vector(y)]
  dim(x) <- old_dims[new_order]
  x
}

# This function is a helper for the function .MergeArrays.
# It expects as inputs two named numeric vectors, and it extends them
# with dimensions of length 1 until an ordered common dimension
# format is reached.
.MergeArrayDims <- function(dims1, dims2) {
  new_dims1 <- c()
  new_dims2 <- c()
  while (length(dims1) > 0) {
    if (names(dims1)[1] %in% names(dims2)) {
      pos <- which(names(dims2) == names(dims1)[1])
      dims_to_add <- rep(1, pos - 1)
      if (length(dims_to_add) > 0) {
        names(dims_to_add) <- names(dims2[1:(pos - 1)])
      }
      new_dims1 <- c(new_dims1, dims_to_add, dims1[1])
      new_dims2 <- c(new_dims2, dims2[1:pos])
      dims1 <- dims1[-1]
      dims2 <- dims2[-c(1:pos)]
    } else {
      new_dims1 <- c(new_dims1, dims1[1])
      new_dims2 <- c(new_dims2, 1)
      names(new_dims2)[length(new_dims2)] <- names(dims1)[1]
      dims1 <- dims1[-1]
    }
  }
  if (length(dims2) > 0) {
    dims_to_add <- rep(1, length(dims2))
    names(dims_to_add) <- names(dims2)
    new_dims1 <- c(new_dims1, dims_to_add)
    new_dims2 <- c(new_dims2, dims2)
  }
  list(new_dims1, new_dims2)
}

# This function takes two named arrays and merges them, filling with
# NA where needed.
# dim(array1)
#          'b'   'c'         'e'   'f'
#           1     3           7     9
# dim(array2)
#    'a'   'b'         'd'         'f'   'g'
#     2     3           5           9     11
# dim(.MergeArrays(array1, array2, 'b'))
#    'a'   'b'   'c'   'e'   'd'   'f'   'g'
#     2     4     3     7     5     9     11
.MergeArrays <- function(array1, array2, along) {
  if (!(is.null(array1) || is.null(array2))) {
    if (!(identical(names(dim(array1)), names(dim(array2))) &&
        identical(dim(array1)[-which(names(dim(array1)) == along)],
                  dim(array2)[-which(names(dim(array2)) == along)]))) {
      new_dims <- .MergeArrayDims(dim(array1), dim(array2))
      dim(array1) <- new_dims[[1]]
      dim(array2) <- new_dims[[2]]
      for (j in 1:length(dim(array1))) {
        if (names(dim(array1))[j] != along) {
          if (dim(array1)[j] != dim(array2)[j]) {
            if (which.max(c(dim(array1)[j], dim(array2)[j])) == 1) {
              na_array_dims <- dim(array2)
              na_array_dims[j] <- dim(array1)[j] - dim(array2)[j]
              na_array <- array(dim = na_array_dims)
              array2 <- abind(array2, na_array, along = j)
              names(dim(array2)) <- names(na_array_dims)
            } else {
              na_array_dims <- dim(array1)
              na_array_dims[j] <- dim(array2)[j] - dim(array1)[j]
              na_array <- array(dim = na_array_dims)
              array1 <- abind(array1, na_array, along = j)
              names(dim(array1)) <- names(na_array_dims)
            }
          }
        }
      }
    }
    if (!(along %in% names(dim(array2)))) {
      stop("The dimension specified in 'along' is not present in the ",
           "provided arrays.")
    }
    array1 <- abind(array1, array2, along = which(names(dim(array1)) == along))
    names(dim(array1)) <- names(dim(array2))
  } else if (is.null(array1)) {
    array1 <- array2
  }
  array1
}

# Takes as input a list of arrays. The list must have named dimensions.
.MergeArrayOfArrays <- function(array_of_arrays) {
  MergeArrays <- .MergeArrays
  array_dims <- (dim(array_of_arrays))
  dim_names <- names(array_dims)

  # Merge the chunks.
  for (dim_index in 1:length(dim_names)) {
    dim_sub_array_of_chunks <- dim_sub_array_of_chunk_indices <- NULL
    if (dim_index < length(dim_names)) {
      dim_sub_array_of_chunks <- array_dims[(dim_index + 1):length(dim_names)]
      names(dim_sub_array_of_chunks) <- dim_names[(dim_index + 1):length(dim_names)]
      dim_sub_array_of_chunk_indices <- dim_sub_array_of_chunks
      sub_array_of_chunk_indices <- array(1:prod(dim_sub_array_of_chunk_indices),
                                          dim_sub_array_of_chunk_indices)
    } else {
      sub_array_of_chunk_indices <- NULL
    }
    sub_array_of_chunks <- vector('list', prod(dim_sub_array_of_chunks))
    dim(sub_array_of_chunks) <- dim_sub_array_of_chunks
    for (i in 1:prod(dim_sub_array_of_chunks)) {
      if (!is.null(sub_array_of_chunk_indices)) {
        chunk_sub_indices <- which(sub_array_of_chunk_indices == i, arr.ind = TRUE)[1, ]
      } else {
        chunk_sub_indices <- NULL
      }
      for (j in 1:(array_dims[dim_index])) {
        new_chunk <- do.call('[[', c(list(x = array_of_arrays),
                                     as.list(c(j, chunk_sub_indices))))
        if (is.null(new_chunk)) {
          stop("Chunks missing.")
        }
        if (is.null(sub_array_of_chunks[[i]])) {
          sub_array_of_chunks[[i]] <- new_chunk
        } else {
          sub_array_of_chunks[[i]] <- MergeArrays(sub_array_of_chunks[[i]],
                                                  new_chunk,
                                                  dim_names[dim_index])
        }
      }
    }
    array_of_arrays <- sub_array_of_chunks
    rm(sub_array_of_chunks)
    gc()
  }

  array_of_arrays[[1]]
}
