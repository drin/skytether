#!/usr/bin/env python
"""
Module for parsing source data files for biological data.
"""

# pylint: disable=wrong-import-order
# pylint: disable=too-few-public-methods
# pylint: disable=multiple-statements
# pylint: disable=consider-using-with
# pylint: disable=broad-except


# ------------------------------
# Dependencies

import os
import sys

import gzip

import scipy.io
import numpy

# classes
from skytether.singlecell import GeneExpression, Annotation

# functions
from skytether.util import NormalizeRecord


# ------------------------------
# Data Parsers

class GeneMetadataParser:
    """ Parser for source files containing metadata for genes. """

    __slots__ = ()

    @classmethod
    def FromHandle(cls, gene_filehandle, has_header=True, delim='\t'):
        """
        Builds a 2D numpy array containing each column and each row in :gene_filehandle:, and
        a 1D numpy array for the file header.

        gene_filehandle: [File]                 A handle for a file containing gene metadata.
        has_header     : [bool] (default: True) A flag to skip the first line or not.
        delim          : [str]  (default: '\t') Delimiter that separates fields of each row.
        """

        if not gene_filehandle or gene_filehandle.closed:
            err_msg = 'Gene list file handle is closed or was never opened.'

            sys.stderr.write(f'{err_msg}\n')
            sys.exit(err_msg)

        gene_header = ['featurekey']
        if has_header:
            gene_header = NormalizeRecord(next(gene_filehandle), delim)

        gene_metadata = [
            numpy.array(NormalizeRecord(gene_line, delim), dtype=str)
            for gene_line in gene_filehandle
        ]

        # ------------------------------
        # Returns a tuple: <array of headers>, <array of gene metadata rows>
        # Note that a row of gene metadata is, itself, an array
        return (
            numpy.array(gene_header  , dtype=str),
            numpy.array(gene_metadata, dtype=str)
        )


class CellMetadataParser:
    """ Parser for source files containing metadata for single cells. """

    __slots__ = ()

    @classmethod
    def FromHandle(cls, cell_metadata_handle, has_header=True, delim='\t', fn_transform=None):
        """
        This function currently hardcodes the column to find the primary key for cells. For the
        MOCA dataset, the file was single column and straightforward. HCA metadata is incredibly
        more complex, but goes through the effort of uniquely identifying a cell using a "cellkey"
        attribute. I do not know how uniqueness of cellkey is determined.

        Currently, no validation of the file contents is done.
        """

        if not cell_metadata_handle or cell_metadata_handle.closed:
            err_msg = 'Cell list file handle is closed or was never opened.'

            sys.stderr.write(f'{err_msg}\n')
            sys.exit(err_msg)

        cell_header = ['cellkey']
        if has_header:
            cell_header = NormalizeRecord(next(cell_metadata_handle), delim)

        cell_metadata = [
            numpy.array(NormalizeRecord(cell_record, delim), dtype=str)
            for cell_record in cell_metadata_handle
        ]

        # ------------------------------
        # Returns a tuple: <array of headers>, <array of cell metadata rows>
        # Note that a row of cell metadata is, itself, an array
        if fn_transform:
            return (
                numpy.array(cell_header                           , dtype=str),
                numpy.array(list(map(fn_transform, cell_metadata)), dtype=str)
            )

        return (
            numpy.array(cell_header  , dtype=str),
            numpy.array(cell_metadata, dtype=str)
        )


# ------------------------------
# file format parsers
class AnnotationCSVParser:
    """ Parser for source files containing annotations (biological metadata). """

    @classmethod
    def FromCSV(cls, path_to_annotations, has_header=True, delim=','):
        """
        Returns annotations represented by a numpy.array object of shape:
            (# of parsed annotations, # of annotation columns)
        """

        with open(path_to_annotations, 'r') as ann_handle:
            # parse headers
            ann_headers = None

            if has_header:
                ann_headers = numpy.array(next(ann_handle).strip().split(delim), dtype=str)

            annotations = [
                numpy.array(ann_line.strip().split(delim), dtype=str)
                for ann_line in ann_handle
            ]

        # return tuple (header, data)
        return Annotation(ann_headers, numpy.array(annotations))


class GeneExpressionMatrixParser:
    """ Parser for single-cell gene expression data. """

    __slots__ = ()

    # ------------------------------
    # Convenience functions used by the parsing API

    @classmethod
    def OpenFilepath(cls, filepath):
        """
        Opens a file path and returns the handle, but also checks if the file exists with a '.gz'
        extension and attempts to open with gzip if the file exists.
        """

        gzip_filepath = f'{filepath}.gz'

        if os.path.isfile(filepath):      return      open(filepath     , 'rb')
        if os.path.isfile(gzip_filepath): return gzip.open(gzip_filepath, 'rb')

        # error is filepaths don't exist
        err_msg = f'Could not find file "{filepath}[.gz]"\n'
        sys.stderr.write(err_msg)
        sys.exit(err_msg)

    @classmethod
    def OpenMTXFromDir(cls, path_to_mtx_root):
        """ Gene expression data in MTX (sparse matrix) format. """
        return cls.OpenFilepath(os.path.join(path_to_mtx_root, 'matrix.mtx'))

    @classmethod
    def OpenGenesFromDir(cls, path_to_mtx_root):
        """ In-order gene metadata corresponding to rows of the MTX file. """
        return cls.OpenFilepath(os.path.join(path_to_mtx_root, 'genes.tsv'))

    @classmethod
    def OpenCellsFromDir(cls, path_to_mtx_root):
        """ In-order cell metadata corresponding to columns of the MTX file. """
        return cls.OpenFilepath(os.path.join(path_to_mtx_root, 'cells.tsv'))

    @classmethod
    def OpenFeaturesFromDir(cls, path_to_mtx_root):
        """ A component dataset that seems specific to recent versions of HCA datasets. """
        return cls.OpenFilepath(os.path.join(path_to_mtx_root, 'features.tsv'))

    @classmethod
    def OpenBarcodesFromDir(cls, path_to_mtx_root):
        """ A component dataset that is specially named in the MOCA project. """
        return cls.OpenFilepath(os.path.join(path_to_mtx_root, 'barcodes.tsv'))

    # ------------------------------
    # Auxiliary Functions
    @classmethod
    def CellIDFromBarcode(cls, sample_name):
        """
        This is a utility function that returns a closure to be mapped onto a list of barcodes.
        """

        def FormatBarcode(cell_record):
            """
            To get 'Cell ID' we prefix the cell barcode with '<sample_name>_' and we remove the
            '-1' from the end.
            """

            return '{}_{}'.format(sample_name, cell_record[0].rstrip('-1'))

        return FormatBarcode

    # ------------------------------
    # Parsing API
    @classmethod
    def GeneExprFromDir(cls, path_to_mtx_dir, has_header=True):
        """
        :path_to_mtx_dir: Path to directory containing matrix data, in MTX format. Files expected
                          in directory:
                              - 'matrix.mtx'
                              - 'genes.tsv'
                              - 'cells.tsv'
                              - 'features.tsv' (optional)

                          Files may be uncompressed, or gzipped ('.gz' suffix).
        """

        # Use scipy's function to read sparse matrix in MTX format.
        with cls.OpenMTXFromDir(path_to_mtx_dir) as matrix_handle:
            matrix_data = scipy.io.mmread(matrix_handle)

        gene_header, gene_metadata = GeneMetadataParser.FromHandle(
            cls.OpenGenesFromDir(path_to_mtx_dir),
            has_header=has_header
        )

        cell_header, cell_metadata = CellMetadataParser.FromHandle(
            cls.OpenCellsFromDir(path_to_mtx_dir),
            has_header=has_header
        )

        return (
            GeneExpression(
                expression_matrix=matrix_data.tocsc(),
                genes=gene_metadata[:,0],
                cells=cell_metadata[:,0]
            ),
            Annotation(
                headers=gene_header,
                annotations=gene_metadata
            ),
            Annotation(
                headers=cell_header,
                annotations=cell_metadata
            ),
        )

    @classmethod
    def GeneExprFromDirCustom(cls, path_to_mtx_dir, has_header=True):
        """
        :path_to_mtx_dir: Path to directory containing matrix data, in MTX format. Files expected
                          in directory:
                              - 'matrix.mtx'
                              - 'genes.tsv'
                              - 'cells.tsv'
                              - 'features.tsv' (optional)

                          Files may be uncompressed, or gzipped ('.gz' suffix).
        """

        try:
            sample_name = os.path.basename(path_to_mtx_dir).split('-')[1]
            closure_transform_barcode = cls.CellIDFromBarcode(sample_name)

        except Exception:
            err_msg = 'Unable to parse sample name from path: {}'.format(path_to_mtx_dir)

            sys.stderr.write(err_msg)
            sys.exit(err_msg)

        # Use scipy's function to read sparse matrix in MTX format.
        matrix_data = scipy.io.mmread(cls.OpenMTXFromDir(path_to_mtx_dir))

        gene_header, gene_metadata = GeneMetadataParser.FromHandle(
            cls.OpenGenesFromDir(path_to_mtx_dir),
            has_header=has_header
        )

        barcode_header, barcode_metadata = CellMetadataParser.FromHandle(
            cls.OpenBarcodesFromDir(path_to_mtx_dir),
            has_header=has_header,
            fn_transform=closure_transform_barcode
        )

        return (
            GeneExpression(
                expression_matrix=matrix_data.tocsc(),
                genes=gene_metadata[:,0],
                cells=barcode_metadata[:,0]
            ),
            Annotation(
                headers=gene_header,
                annotations=gene_metadata
            ),
            Annotation(
                headers=barcode_header,
                annotations=barcode_metadata
            ),
        )
