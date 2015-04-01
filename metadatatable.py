#!/usr/bin/env python

__author__ = 'hofmann'

import os
import gzip


class MetadataTable:
	"""Reading and writing a meta table"""
	def __init__(self, separator="\t", logger=None):
		self._logger = logger
		self._number_of_rows = 0
		self._header = []
		self._meta_table = {}
		self._separator = separator

	def clear(self):
		self._header = []
		self._number_of_rows = 0
		self._meta_table = {}

	def remove_empty_columns(self):
		new_header = list(self._header)
		for title in self._header:
			column = set(self.get_column(title))
			column = [value.strip() for value in column]
			if len(column) == 1 and '' in column:
				new_header.remove(title)
		self._header = new_header

	def read(self, file_path, head=True, comment_line=None):
		if comment_line is None:
			comment_line = ['#']

		assert isinstance(file_path, basestring)
		assert isinstance(comment_line, basestring)
		assert isinstance(head, bool)

		fopen = open
		if file_path.endswith(".gz"):
			fopen = gzip.open

		self.clear()
		if not os.path.isfile(file_path):
			if self._logger:
				self._logger.error("[MetaTable] no file found at: '{}'".format(file_path))
			return
		file_handler = fopen(file_path)
		if head:
			self._header = file_handler.readline().strip().split(self._separator)
			for column_name in self._header:
				self._meta_table[column_name] = []
		for line in file_handler:
			if line[0] in comment_line or len(line.strip().strip('\r')) == 0:
				continue
			self._number_of_rows += 1
			row = line.split(self._separator)
			for index in range(0, len(row)):
				if head:
					self._meta_table[self._header[index]].append(row[index].rstrip('\n'))
				else:
					if index not in self._meta_table:
						self._meta_table[index] = []
					self._meta_table[index].append(row[index].rstrip('\n'))
		file_handler.close()

	def write(self, file_path, head=True, compression_level=0,
			  include_value_list=None,
			  exclude_value_list=None,
			  key_exclude_header=None):

		assert isinstance(file_path, basestring)
		assert 0 <= compression_level < 10

		if compression_level > 0:
			file_handler = gzip.open(file_path, "w", compression_level)
		else:
			file_handler = open(file_path, "w")

		if head:
			header = self._separator.join(self._header)
			file_handler.write(header + '\n')
		for row_number in range(0, self._number_of_rows):
			row = []
			if key_exclude_header is not None:
				if include_value_list is not None and self._meta_table[key_exclude_header][row_number] not in include_value_list:
					continue
				if exclude_value_list is not None and self._meta_table[key_exclude_header][row_number] in exclude_value_list:
					continue
			for head in self._header:
				if len(self._meta_table[head]) > row_number:
					row.append(str(self._meta_table[head][row_number]))
				else:
					row.append('')
			file_handler.write(self._separator.join(row) + '\n')
		file_handler.close()

	def get_header(self):
		return list(self._header)

	def get_number_of_rows(self):
		return self._number_of_rows

	def get_entry_index(self, column_name, entry_name):
		assert isinstance(column_name, (basestring, int))
		if entry_name in self._meta_table[column_name]:
			return self._meta_table[column_name].index(entry_name)
		else:
			return None

	def has_column(self, column_name):
		assert isinstance(column_name, (basestring, int))
		if column_name in self._meta_table:
			return True
		else:
			return False

	def get_column(self, column_name):
		assert isinstance(column_name, (basestring, int))
		if column_name in self._meta_table:
			return list(self._meta_table[column_name])
		else:
			return None

	def get_empty_column(self, default_value=''):
		assert isinstance(default_value, basestring)
		return [default_value] * self._number_of_rows

	def set_column(self, column_name=None, values=None):
		if column_name is None:
			column_name = len(self._header)
		assert isinstance(column_name, (basestring, int))

		if values is None:
			values = self.get_empty_column()
		assert isinstance(values, list)

		if column_name not in self._header:
			self._header.append(column_name)
		self._meta_table[column_name] = values

	def get_new_row(self):
		row = {}
		for head in self._header:
			row[head] = ''
		return row

	def add_row(self, row):
		assert isinstance(row, (list, dict))
		if isinstance(row, dict):
			diff = set(self._header).difference(set(row.keys()))
			if len(diff) != 0:
				if self._logger:
					self._logger.error("[MetaTable] Bad header, could not add row!")
				return
			for head in self._header:
				self._meta_table[head].append(row[head])
		else:
			assert len(row) == len(self._header)
			for index_column in range(len(row)):
				self._meta_table[self._header[index_column]].append(row[index_column])
		self._number_of_rows += 1

	def get_cell_value(self, key_header, key_value, value_header):
		assert isinstance(key_header, (basestring, int))
		assert isinstance(value_header, (basestring, int))
		if key_header not in self._header or value_header not in self._header:
			if self._logger:
				self._logger.error("[MetaTable] Bad header, could not get value!")
			return None
		index = self.get_entry_index(key_header, key_value)
		if index is not None:
			return self._meta_table[value_header][index]
		return None

	def validate_headers(self, list_of_header):
		for header in list_of_header:
			if header not in self._header:
				return False
		return True

	def concatenate(self, meta_table, strict=True):
		assert isinstance(meta_table, MetadataTable)
		if len(self._header) == 0:
			strict = False
		if strict:
			if not self.validate_headers(meta_table.get_header()) or not meta_table.validate_headers(self._header):
				if self._logger:
					self._logger.error("[MetaTable] header are not identical!")
				return
			for header in self._header:
				self._meta_table[header].extend(meta_table.get_column(header))
		else:
			for header in meta_table.get_header():
				if header in self._header:
					self._meta_table[header].extend(meta_table.get_column(header))
				else:
					new_column = self.get_empty_column()
					new_column.extend(meta_table.get_column(header))
					self.set_column(new_column, header)
		self._number_of_rows += meta_table.get_number_of_rows()
		for header in self._header:
			if len(self._meta_table[header]) < self._number_of_rows:
				self._meta_table[header].extend([''] * (self._number_of_rows - len(self._meta_table[header])))

	def reduce_to_subset(self, key_header, list_of_values):
		assert isinstance(key_header, (basestring, int))
		assert key_header in self._header
		assert isinstance(list_of_values, list)
		new_meta_table = {}
		for header in self._header:
			new_meta_table[header] = []
		column = self.get_column(key_header)
		for index, value in enumerate(column):
			if value not in list_of_values:
				continue
			for header in self._header:
				new_meta_table[header].append(self._meta_table[header][index])
		self._meta_table = new_meta_table
		self._number_of_rows = len(self._meta_table[key_header])

	def get_map(self, key_header, value_header):
		assert isinstance(key_header, (basestring, int))
		assert isinstance(value_header, (basestring, int))
		assert key_header in self._header
		assert value_header in self._header

		if key_header not in self._meta_table:
			self._logger.error("[MetaTable] key_header '{}' not available!".format(key_header))
			return None
		if value_header not in self._meta_table:
			self._logger.error("[MetaTable] value_header not available!")
			return None
		new_map = {}
		if len(self._meta_table) < 2:
			return new_map
		row_keys = self._meta_table[key_header]
		row_values = self._meta_table[value_header]
		for index, key in enumerate(row_keys):
			new_map[key] = row_values[index]
		return new_map

	def rename_column(self, old_id, new_id):
		if old_id not in self._header:
			self._logger.error("[MetaTable] column name '{}' not available!")
			return None
		self._header[self._header.index(old_id)] = new_id
		self._meta_table[new_id] = self._meta_table.pop(old_id)
