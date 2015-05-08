#!/usr/bin/env python

__author__ = 'hofmann'
__version__ = '0.0.3'

import os
import io
import StringIO
import gzip
from scripts.loggingwrapper import LoggingWrapper


class MetadataTable(object):
	_label = "MetadataTable"
	"""Reading and writing a meta table"""
	def __init__(self, separator="\t", logfile=None, verbose=True):
		"""
			Handle tab separated files

			@attention:

			@param separator: default character assumed to separate values in a file
			@type separator: str | unicode
			@param logfile: file handler or file path to a log file
			@type logfile: file | io.FileIO | StringIO.StringIO | basestring
			@param verbose: Not verbose means that only warnings and errors will be past to stream
			@type verbose: bool

			@return: None
			@rtype: None
		"""
		assert logfile is None or isinstance(logfile, basestring) or self._is_stream(logfile)
		assert isinstance(separator, basestring), "separator must be string"
		assert isinstance(verbose, bool), "verbose must be true or false"

		self._logger = LoggingWrapper(self._label, verbose=verbose)
		if logfile is not None:
			self._logger.set_log_file(logfile)

		self._number_of_rows = 0
		self._meta_table = {}
		self._separator = separator
		self._list_of_column_names = []

	def __exit__(self, type, value, traceback):
		self.close()

	def __enter__(self):
		return self

	def close(self):
		self._logger.close()

	@staticmethod
	def _is_stream(stream):
		"""
			Test for stream

			@param stream: Any kind of stream type
			@type stream: file | io.FileIO | StringIO.StringIO

			@return: True if stream
			@rtype: bool
		"""
		return isinstance(stream, (file, io.FileIO, StringIO.StringIO)) or stream.__class__ is StringIO.StringIO

	def clear(self):
		self._number_of_rows = 0
		self._meta_table = {}
		self._list_of_column_names = []

	def _has_unique_columns(self, list_of_column_names=None):
		if list_of_column_names is None:
			list_of_column_names = self._list_of_column_names
		return len(list_of_column_names) == len(set(list_of_column_names))

	def remove_empty_columns(self):
		for column_name in self.get_column_names():
			column = set(self.get_column(column_name))
			column = [value.strip() for value in column]
			if len(column) == 1 and '' in column:
				self._meta_table.pop(column_name)
				index = self._list_of_column_names.index(column_name)
				self._list_of_column_names.pop(index)

	def read(self, file_path, separator=None, column_names=False, comment_line=None):
		"""
			Reading comma or tab separated values in a file as table

			@param file_path: path to file to be opened
			@type file_path: str | unicode
			@param separator: default character assumed to separate values in a file
			@type separator: str | unicode
			@param column_names: True if column names available
			@type column_names: bool
			@param comment_line: character or list of character indication comment lines
			@type comment_line: str | unicode | list[str|unicode]

			@return: None
			@rtype: None
		"""
		if comment_line is None:
			comment_line = ['#']
		elif isinstance(comment_line, basestring):
			comment_line = [comment_line]

		if separator is None:
			separator = self._separator

		assert isinstance(file_path, basestring)
		assert isinstance(separator, basestring)
		assert isinstance(comment_line, list)
		assert isinstance(column_names, bool)

		fopen = open
		if file_path.endswith(".gz"):
			fopen = gzip.open

		self.clear()
		if not os.path.isfile(file_path):
			msg = "No file found at: '{}'".format(file_path)
			self._logger.error(msg)
			raise IOError(msg)

		with fopen(file_path) as file_handler:
			self._logger.info("Reading file: '{}'".format(file_path))

			# read column names
			if column_names:
				row = file_handler.readline().rstrip('\n').rstrip('\r')
				list_of_column_names = row.split(separator)
				assert self._has_unique_columns(list_of_column_names), "Column names must be unique!"
				self._list_of_column_names = list_of_column_names
				for column_name in self._list_of_column_names:
					self._meta_table[column_name] = []

			# read rows
			row_count = 0
			for line in file_handler:
				row_count += 1
				row = line.rstrip('\n').rstrip('\r')
				if line[0] in comment_line or len(row) == 0:
					continue
				self._number_of_rows += 1
				row_cells = row.split(separator)
				number_of_columns = len(self.get_column_names())
				if number_of_columns != 0 and number_of_columns != len(row_cells):
					msg = "Format error. Bad number of values in row {}".format(row_count)
					self._logger.error(msg)
					raise ValueError(msg)
				for index, value in enumerate(row_cells):
					if column_names:
						column_name = self._list_of_column_names[index]
					else:
						column_name = index
						if column_name not in self._meta_table:
							self._meta_table[column_name] = []
					self._meta_table[column_name].append(row_cells[index].rstrip('\n').rstrip('\r'))

			if not column_names:
				self._list_of_column_names = sorted(self._meta_table.keys())

	def write(
		self, file_path, separator=None, column_names=True, compression_level=0,
		exclude=None, value_list=None, key_column_names=None):
		"""
			Write tab separated files

			@attention: No comments will be written

			@param file_path: path to file to be opened
			@type file_path: str | unicode
			@param separator: file handler or file path to a log file
			@type separator: str | unicode
			@param column_names: True if column names should be written
			@type column_names: bool
			@param compression_level: any value above 0 will compress files
			@type compression_level: int | long
			@param exclude: If True, rows with a value in the value_list at the key_column_names are removed, False: all others are removed
			@type exclude: None | bool
			@param value_list:
			@type value_list: list[str|unicode]
			@param key_column_names: column name of excluded or included rows
			@type key_column_names: str | unicode

			@return: None
			@rtype: None
		"""

		if separator is None:
			separator = self._separator

		assert isinstance(file_path, basestring)
		assert isinstance(separator, basestring)
		assert isinstance(column_names, bool)
		assert isinstance(compression_level, (int, long))
		assert 0 <= compression_level < 10
		assert exclude is None or isinstance(exclude, bool)
		assert value_list is None or isinstance(value_list, list)
		assert key_column_names is None or isinstance(value_list, basestring)

		if compression_level > 0:
			file_handler = gzip.open(file_path, "w", compression_level)
		else:
			file_handler = open(file_path, "w")

		if column_names:
			if not isinstance(self._list_of_column_names[0], basestring):
				header = separator.join([str(index) for index in self._list_of_column_names])
			else:
				header = separator.join(self._list_of_column_names)
			file_handler.write(header + '\n')
		for row_number in range(0, self._number_of_rows):
			if exclude is not None:
				if not exclude and self._meta_table[key_column_names][row_number] not in value_list:
					continue
				if exclude and self._meta_table[key_column_names][row_number] in value_list:
					continue

			row = []
			for column_names in self._list_of_column_names:
				row.append(str(self._meta_table[column_names][row_number]))
			file_handler.write(separator.join(row) + '\n')
		file_handler.close()

	def get_column_names(self):
		"""
			Get list of column names

			@attention: returns list of indexes if no column names available

			@return: List of column names or indexes
			@rtype: list[str|int]
		"""
		return list(self._list_of_column_names)

	def get_number_of_rows(self):
		"""
			Get number of rows

			@attention:

			@return: Number of rows
			@rtype: int
		"""
		return self._number_of_rows

	def get_number_of_columns(self):
		"""
			Get number of columns

			@attention:

			@return: Number of rows
			@rtype: int
		"""
		return len(self.get_column_names())

	def get_row_index_of_value(self, value, column_name):
		"""
			Get index of value in a column

			@attention:

			@param value: value in column
			@type value: str | unicode
			@param column_name: column name
			@type column_name: int | long | str | unicode

			@return: index of value in a column, None if not there
			@rtype: None | int
		"""
		assert isinstance(column_name, (basestring, int, long))
		assert self.has_column(column_name), "Column '{}' not found!".format(column_name)

		if value in self._meta_table[column_name]:
			return self._meta_table[column_name].index(value)
		else:
			return None

	def has_column(self, column_name):
		"""
			Get index of value in a column

			@attention:

			@param column_name: column name
			@type column_name: int | long | str | unicode

			@return: True if column available
			@rtype: bool
		"""
		assert isinstance(column_name, (basestring, int, long))

		if column_name in self._meta_table:
			return True
		else:
			return False

	def get_column(self, column_name):
		"""
			Get a column

			@attention: use index if no name available

			@param column_name: column name
			@type column_name: int | long | str | unicode

			@return: Cell values of a column
			@rtype: list[str|unicode]
		"""
		assert isinstance(column_name, (basestring, int, long))

		if column_name in self._meta_table:
			return list(self._meta_table[column_name])
		else:
			return None

	def get_empty_column(self, default_value=''):
		"""
			Get a empty column with the same number of rows as the current table

			@attention: empty list if number of rows is zero

			@param default_value: column name
			@type default_value: str | unicode

			@return: Column with cell values set to default value
			@rtype: list[str|unicode]
		"""
		assert isinstance(default_value, basestring)
		return [default_value] * self._number_of_rows

	def get_empty_row(self, default_value='', as_list=False):
		"""
			Get a empty column with the same number of rows as the current table

			@attention: empty list if number of rows is zero

			@param default_value: column name
			@type default_value: str | unicode
			@param as_list: return a list if true
			@type as_list: bool

			@return: Column with cell values set to default value
			@rtype: dict | list
		"""
		assert isinstance(default_value, basestring)
		assert isinstance(as_list, bool)
		if as_list:
			return [default_value] * len(self._list_of_column_names)
		row = {}
		for column_name in self._list_of_column_names:
			row[column_name] = default_value
		return row

	def insert_column(self, list_of_values=None, column_name=None):
		"""
			Insert a new column or overwrite an old one.

			@attention: if column_name exists, it will be overwritten

			@param list_of_values: Cell values of table column
			@type list_of_values: list[str|unicode]
			@param column_name: column name or index
			@type column_name: int | long | str | unicode

			@return: Nothing
			@rtype: None
		"""
		if column_name is None:
			column_name = len(self._list_of_column_names)
		assert isinstance(column_name, (basestring, int, long))
		# assert len(values) == self._number_of_rows, ""

		if list_of_values is None:
			list_of_values = self.get_empty_column()
		assert isinstance(list_of_values, list)
		assert len(list_of_values) == self._number_of_rows, "Bad amount of values: {}/{}".format(
			len(list_of_values), self._number_of_rows)

		if column_name not in self._list_of_column_names:
			self._list_of_column_names.append(column_name)
		self._meta_table[column_name] = list_of_values

	def insert_row(self, row):
		"""
			Insert a new row.

			@attention:

			@param row: Cell values of a row
			@type row: list[str|unicode] | dict

			@return: Nothing
			@rtype: None
		"""
		assert isinstance(row, (list, dict))
		assert len(row) == len(self._list_of_column_names)
		if isinstance(row, dict):
			diff = set(self._list_of_column_names).difference(set(row.keys()))
			if len(diff) != 0:
				msg = "Bad column names '{}', could not add row!".format(", ".join(diff))
				self._logger.error(msg)
				raise ValueError(msg)
			for column_name in self._list_of_column_names:
				self._meta_table[column_name].append(row[column_name])
		else:
			# assert len(row) == len(self._header)
			for index_column in range(len(row)):
				self._meta_table[self._list_of_column_names[index_column]].append(row[index_column])
		self._number_of_rows += 1

	def get_cell_value(self, key_column_name, key_value, value_column_name):
		"""
			Get the cell value at the index of a key in a key column

			@attention:

			@param key_column_name: column name
			@type key_column_name: str | unicode | int | long
			@param value_column_name: column name
			@type value_column_name: str | unicode | int | long
			@param key_value: key cell value
			@type key_value: str | unicode

			@return: None if key value is not there
			@rtype: str | unicode | None
		"""
		assert isinstance(key_column_name, (basestring, int, long))
		assert isinstance(value_column_name, (basestring, int, long))
		assert self.has_column(key_column_name), "Column '{}' not found!".format(key_column_name)
		assert self.has_column(value_column_name), "Column '{}' not found!".format(value_column_name)

		index = self.get_row_index_of_value(key_value, key_column_name)
		if index is not None:
			return self._meta_table[value_column_name][index]
		return None

	def validate_column_names(self, list_of_column_names):
		"""
			Validate that a list of column names exists in the loaded table

			@attention:

			@param list_of_column_names: column name
			@type list_of_column_names: list[str|unicode]

			@return: True if all column names exist
			@rtype: bool
		"""
		assert isinstance(list_of_column_names, list)

		list_of_invalid_column_names = []
		for column_name in list_of_column_names:
			if not self.has_column(column_name):
				list_of_invalid_column_names.append(column_name)
				self._logger.info("Invalid columns: {}".format(", ".join(list_of_invalid_column_names)))
		if len(list_of_invalid_column_names) > 0:
			return False
		return True

	def concatenate(self, meta_table, strict=True):
		"""
			Concatenate two metadata tables

			@attention:

			@param meta_table: column name
			@type meta_table: MetadataTable
			@param strict: if true, both tables must have the same column names, else empty cells will be added where needed
			@type strict: bool

			@return: Nothing
			@rtype: None
		"""
		assert isinstance(meta_table, MetadataTable)
		assert isinstance(strict, bool)

		if len(self._list_of_column_names) == 0:
			strict = False
		if strict:
			valid_foreign_column_names = self.validate_column_names(meta_table.get_column_names())
			valid_own_column_names = meta_table.validate_column_names(self._list_of_column_names)
			if not valid_foreign_column_names or not valid_own_column_names:
				msg = "Column names are not identical!"
				self._logger.error(msg)
				raise ValueError(msg)
			for column_name in self._list_of_column_names:
				self._meta_table[column_name].extend(meta_table.get_column(column_name))
		else:
			for column_name in meta_table.get_column_names():
				if column_name not in self._list_of_column_names:
					self.insert_column(self.get_empty_column(), column_name)
				self._meta_table[column_name].extend(meta_table.get_column(column_name))

		self._number_of_rows += meta_table.get_number_of_rows()

		for column_name in self._list_of_column_names:
			if len(self._meta_table[column_name]) < self._number_of_rows:
				self._meta_table[column_name].extend([''] * (self._number_of_rows - len(self._meta_table[column_name])))

	def reduce_rows_to_subset(self, list_of_values, key_column_name):
		"""
			Keep rows at key values of a column

			@attention:

			@param list_of_values: Cell values of table column
			@type list_of_values: list[str|unicode]
			@param key_column_name: Column name
			@type key_column_name: str | unicode

			@return: Nothing
			@rtype: None
		"""

		assert isinstance(key_column_name, (basestring, int, long))
		assert isinstance(list_of_values, list)
		assert self.has_column(key_column_name), "Column '{}' not found!".format(key_column_name)

		new_meta_table = {}
		for column_name in self._list_of_column_names:
			new_meta_table[column_name] = []
		column = self.get_column(key_column_name)
		for index, value in enumerate(column):
			if value not in list_of_values:
				continue
			for column_name in self._list_of_column_names:
				new_meta_table[column_name].append(self._meta_table[column_name][index])
		self._meta_table = new_meta_table
		self._number_of_rows = len(self._meta_table[key_column_name])

	def get_map(self, key_column_name, value_column_name):
		"""
			Keep rows at key values of a column

			@attention:

			@param key_column_name: Column name
			@type key_column_name: str | unicode | int | long
			@param value_column_name: Column name
			@type value_column_name: str | unicode | int | long

			@return: map
			@rtype: dict[str|unicode, str|unicode]
		"""

		assert isinstance(key_column_name, (basestring, int, long))
		assert isinstance(value_column_name, (basestring, int, long))
		assert self.has_column(key_column_name), "Column '{}' not found!".format(key_column_name)
		assert self.has_column(value_column_name), "Column '{}' not found!".format(value_column_name)

		if key_column_name not in self._meta_table:
			self._logger.error("Column name '{}' not available!".format(key_column_name))
			return None
		if value_column_name not in self._meta_table:
			self._logger.error("Column name '{}' not available!".format(value_column_name))
			return None
		new_map = {}
		if len(self._meta_table) < 2:
			return new_map
		row_keys = self._meta_table[key_column_name]
		row_values = self._meta_table[value_column_name]
		for index, key in enumerate(row_keys):
			if key in new_map:
				self._logger.warning("Key column is not unique! Key: '{}'".format(key))
			new_map[key] = row_values[index]
		return new_map

	def rename_column(self, old_column_name, new_column_name):
		"""
			Keep rows at key values of a column

			@attention:

			@param old_column_name: Column name
			@type old_column_name: str | unicode
			@param new_column_name: Column name
			@type new_column_name: str | unicode

			@return: Nothing
			@rtype: None
		"""
		assert isinstance(old_column_name, (basestring, int, long))
		assert isinstance(new_column_name, (basestring, int, long))
		assert self.has_column(old_column_name), "Column '{}' not found!".format(old_column_name)

		self._list_of_column_names[self._list_of_column_names.index(old_column_name)] = new_column_name
		self._meta_table[new_column_name] = self._meta_table.pop(old_column_name)
