############################################################
#### CHECK FOR INSTALLED PACKAGES AND INSTALL IF NEEDED ####
############################################################

check.and.install <- function(p){
    needed.packages <- p[!(p %in% installed.packages()[,"Package"])]

    ## Install missing packages
    if (length(needed.packages) > 0) install.packages(needed.packages)

    ## Load all packages
    for (i in 1:length(p)){
        eval(parse(text=paste0("library(",p[i],")")))
    }
}

#' Alternate form of `check.and.install` that is more conservative
#'
#' Rather than automatically attaching the checked packages in `pkg_list`,
#' this utility checks with the user if they want to install the packages.
#' In non-interactive modes, a warning will be generated indicating that
#' packages were installed.
#'
#' @param pkg_list A character vector of package names, to be installed from CRAN
#' @param attach_pkgs A logical to indicate whether or not to use `library()` after installing the packages. Generally recommended to be `FALSE` so that the user has direct control over their search path
#' @return A logical vector, the same length as `pkg_list`, indicating whether or not a package is installed.
check_install <- function(pkg_list, attach_pkgs = FALSE) {
    stopifnot(is.character(pkg_list))

    if (length(pkg_list) < 1L) {
        return(NA)
    }

    needs_install <- vapply(pkg_list, function(p) !requireNamespace(p, quietly = TRUE), logical(1L))
    ni_list <- pkg_list[needs_install]

    if (length(ni_list) > 0L) {
        if (interactive()) {
            cat("These packages need to be installed:\n", paste0(paste0("  - ", ni_list), collapse = "\n"), sep = "")
            ans <- trimws(readline("Do you confirm this installation? [y/n] "))

            if (isTRUE(grepl("y", ans, ignore.case = TRUE))) {
                install.packages(ni_list, quiet = TRUE)
            }
        } else {
            warning("Some packages were installed: ", paste0(ni_list, collapse = ", "), call. = FALSE, immediate. = TRUE)

            install.packages(ni_list, quiet = TRUE)
        }
    }

    are_installed <- vapply(pkg_list, function(p) requireNamespace(p, quietly = TRUE), logical(1L))

    if (isTRUE(attach_pkgs)) {
        for (p in pkg_list[are_installed]) {
            library(p, character.only = TRUE)
        }
    }

    invisible(are_installed)
}
