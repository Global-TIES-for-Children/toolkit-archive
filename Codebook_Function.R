##############################################################################################################
###################### CODE-BOOK CREATION FUNCTION ###########################################################
##############################################################################################################

#' Generate a formatted codebook for finalized metadata in an item mapping sheet
#'
#' @param USER Defunct. Used to hold a reference to the current user
#' @param MASTERCODEBOOK Path to the input codebook spreadsheet
#' @param ASSESSMENTS Character vector of all the target assessment groups to bundle together under each index section
#' @param ADESCRIPTION OPTIONAL: Name of the column that contains the description of the full index section, in your mapping spreadsheet
#' @param FINALNAMES Name of the column in the spreadsheet that contains all the variable names that should appear in the codebook
#' @param ITEMDES Name of the column in the spreadsheet that contains the descriptions of the individual items
#' @param CODING Name of the column in the codebook spreadsheet that contains the coding (value/label relations) that should appear in the codebook
#' @param SUBGROUP Name of the column that contains the subgroups names for related items that SHARE THE EXACT SAME CODING/SCALE (if applicable, otherwise don't fill in)
#' @param SUBDESCRIPTION Name of the column that contains the common description/question for related items that SHARE THE EXACT SAME CODING/SCALE (if applicable, otherwise don't fill in)
#' @param TITLE The output file name
#' @param OUTPATH The director to where the output file will go
#' @param DATABASE OPTIONAL: If you want basic deecriptives and histograms in your codebook, you must specify the path to the database described by the codebook. 
#' Note that the contents of the database should be consisten with all the final names and coding specified in the Mapping Format.
#' @param LEAVEOUT OPTIONAL: If there are a few variables within any given assessments that belong to only one codebook but not another (e.g. year specific variables that are only relevant for Y1 codebook but not Y2 codebook) 
#' then specify the "leaveout" indicator here. You should create a leeaveout column in the mapping sheet that flags the cases that belong to each year. See example.
#' @param DROPCASE OPTIONAL: Name of the Reduction column or of the column that indicates if certain variables were dropped from the target database during the cleaning process
#' This should only be used in cases were the names in the FINALNAMES column are not fully contained in the database.
#' @param PRESENCE.COL OPTIONAL: Name of the column that contains the original names of the variables in the ONE time-point that you wish to generate a codebook for.
#' If you provide this name then the codebook will only contain the names of variables that were collected in that ONE wave.
#' @export
##############################################################################################################
###################### CODE-BOOK CREATION FUNCTION ###########################################################
##############################################################################################################

codebook <- function(USER, MASTERCODEBOOK, ASSESSMENTS, GROUP=NULL,FINALNAMES, ITEMDES,CODING=NULL,SUBGROUP=NULL,SUBDESCRIPTION=NULL,SUBNOTE=NULL,TITLE, 
                     OUTPATH, DATABASE = NULL, LEAVEOUT=NULL,ADESCRIPTION=NULL,DROPCASE=NULL, FORMAT.PATH=NULL,FUN.DIR=NULL, PRESENCE.COL=NULL) {
    if(is.null(FORMAT.PATH)){
        print(paste0("You forgot to specify a file to be used as an example for formatting under 'FORMAT.PATH=', so I'm defaulting to:", box_root,"Documentation/Functions/CodebookF test/Codebook Template.docx" ))
        FORMAT.PATH <- file.path(box_root,"Documentation/Functions/CodebookF test/Codebook Template.docx")}
    
    if(is.null(FUN.DIR)){
        print(paste0("You forgot to specify a path for the General Functions folder under 'FUN.DIR=', so I'm defaulting to:", box_root,"General Functions/" ))
        FUN.DIR <- file.path(box_root,"General Functions/")}
    
    source(file.path(FUN.DIR, "packages_check.R"))
    p <- c("ggplot2","brew","crayon")
    check.and.install(p)
    
    source_file <- file.path(FUN.DIR, "Codebook_Creation_Code.Rmd")
    temp_file   <- file.path(OUTPATH, "Codebook_Creation_Code_TEMP.Rmd")
    
    # Lifted from https://stackoverflow.com/questions/23449319/yaml-current-date-in-rmarkdown
    brew::brew(source_file, temp_file); on.exit(unlink(temp_file))
    
    rmarkdown::render(temp_file, 
        output_format = "word_document",
        output_file =  paste(TITLE,".docx", sep = ''),
        # output_format = "pdf_document",
        # output_file =  paste(TITLE,".pdf", sep=''),
        output_dir = OUTPATH,
        encoding = "latin1"
    )
}


