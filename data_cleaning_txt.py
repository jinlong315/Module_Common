import datetime
import shutil
import pandas as pd
import re
from pathlib import Path

# set display columns and rows
pd.set_option("display.max_rows", 5)
pd.set_option("display.max_columns", 20)


def get_next_n_months(start_date, next_n=13):
	"""
    Get next N months date based on given start date
    :return: list | list of next N months
    """

	# initialization for start date
	if start_date is None:
		start_date = datetime.datetime.now()

	# define list to save result
	list_date = []
	for i in range(0, next_n, 1):
		next_year = start_date.year + (start_date.month + i - 1) // 12
		next_month = (start_date.month + i) % 12 or 12

		next_date = datetime.datetime(year=next_year, month=next_month, day=1)
		list_date.append(datetime.datetime.strftime(next_date, "%Y-%m-%d"))

	return list_date


def determine_pr_po_type(row):
	"""
    Determine PR / PO type for MM_PUR_PR and MM_PUR_PO
    :param row: str | row data
    :return: int | type id
    """

	if (row["material_number"] != "") and (row["order"] == "") and (row["cost_center"] == "") and (
			row["WBS_element"] == ""):
		return 1
	elif (row["order"] != "") and (row["order"].startswith("4")):
		return 2
	elif (row["order"] != "") and (row["order"].startswith("2")):
		return 3
	elif (row["cost_center"] != ""):
		return 4
	elif (row["WBS_element"] != ""):
		return 5


def number_processing(old_str):
	"""
    Deal with number sting
    :param old_str: str | number like string
    :return: str | string with new format
    """

	# if old_str is not None
	old_str = str(old_str).strip()
	if len(old_str) > 0:

		# replace "," in string
		new_str = old_str.replace(",", "")

		# put "-" in the first place
		if new_str[-1] == "-":
			new_str = "".join(["-", new_str[:-1]])

		return new_str

	else:
		return old_str


class SAPVP:

	def __init__(self, dir_pcl):
		"""
        Initialization for variable
        :param dir_pcl: absolute directory of raw data
        """

		self.dir_pcl = dir_pcl

		# get basic information for raw data
		self.file_name = Path(self.dir_pcl).stem
		self.creation_time = datetime.datetime.fromtimestamp(Path(self.dir_pcl).stat().st_ctime)
		self.last_modified_time = datetime.datetime.fromtimestamp(Path(self.dir_pcl).stat().st_ctime)

	def vp_02_vevw(self):
		"""
        :return: DataFrame with cleaned data
        """

		# open file
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"client": [],
			                     "handle": [],
			                     "object_type": [],
			                     "object_key": [],
			                     "time_stamp": [],
			                     "direction": []
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[2:8]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["object_key"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# reset index
			df_merged.drop_duplicates(inplace=True)
			df_merged = df_merged.drop(df_merged[df_merged["object_key"] == "*"].index)
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			return df_merged

	def vp_03_mb51(self):
		"""
        :return: DataFrame with cleaned data
        """

		# open file
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"material_number": [],
			                     "material_desc": [],
			                     "plant": [],
			                     "name": [],
			                     "stock_location": [],
			                     "movement_type_desc": [],
			                     "movement_type": [],
			                     "supplier": [],
			                     "PO": [],
			                     "material_document": [],
			                     "batch": [],
			                     "posting_date": [],
			                     "quantity": [],
			                     "amount_LC": [],
			                     "user": [],
			                     "document_header_desc": [],
			                     "reference": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:18]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["material_number"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# reset index
			df_merged.drop_duplicates(inplace=True)
			df_merged = df_merged.drop(df_merged[df_merged["material_number"] == "*"].index)
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["posting_date"] = pd.to_datetime(df_merged["posting_date"], format="%Y.%m.%d", errors="coerce")
			df_merged["quantity"] = df_merged["quantity"].apply(number_processing)

			return df_merged

	def vp_04_mb52(self):
		"""
        :return: DataFrame with cleaned data
        """

		# open file
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"material_number": [],
			                     "material_desc": [],
			                     "material_type": [],
			                     "batch": [],
			                     "S_loc": [],
			                     "unrestricted": [],
			                     "value_unrestricted": [],
			                     "blocked_quantity": [],
			                     "blocked_value": [],
			                     "S": [],
			                     "special_block_number": []
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:12]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["material_number"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# reset index
			df_merged.drop_duplicates(inplace=True)
			df_merged = df_merged.drop(df_merged[df_merged["material_number"] == "*"].index)
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["unrestricted"] = df_merged["unrestricted"].apply(number_processing)
			df_merged["value_unrestricted"] = df_merged["unrestricted"].apply(number_processing)
			df_merged["blocked_quantity"] = df_merged["blocked_quantity"].apply(number_processing)
			df_merged["blocked_value"] = df_merged["blocked_value"].apply(number_processing)

			# data type
			df_merged["unrestricted"] = pd.to_numeric(df_merged["unrestricted"])
			df_merged["value_unrestricted"] = pd.to_numeric(df_merged["value_unrestricted"])
			df_merged["blocked_quantity"] = pd.to_numeric(df_merged["blocked_quantity"])
			df_merged["blocked_value"] = pd.to_numeric(df_merged["blocked_value"])
			df_merged["year_month_day"] = pd.to_datetime(df_merged["year_month_day"], format="%Y-%m-%d",
			                                             errors="coerce")

			return df_merged

	def vp_05_lx02(self):
		"""
        :return: DataFrame with cleaned data
        """

		# open file
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# warehouse number
			warehouse_number = data[2].strip()[-3:]

			# define dictionary to save raw data
			dict_cleaned_data = {"material_number": [],
			                     "plant": [],
			                     "S_loc": [],
			                     "S": [],
			                     "batch": [],
			                     "S_1": [],
			                     "special_block_number": [],
			                     "material_desc": [],
			                     "storage_unit": [],
			                     "type": [],
			                     "storage_bin": [],
			                     "available_stock": [],
			                     "pick_quantity": [],
			                     "stock_for_put_away": [],
			                     "unit": [],
			                     "GR_date": [],
			                     "GR_number": [],
			                     "to_it": [],
			                     "to_quant": [],
			                     "to_number": []
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:21]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["material_number"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day, warehouse_number]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day",
					         "warehouse_number"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# reset index
			df_merged.drop_duplicates(inplace=True)
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["GR_date"] = pd.to_datetime(df_merged["GR_date"], format="%Y.%m.%d", errors="coerce")
			df_merged["available_stock"] = df_merged["available_stock"].apply(number_processing)
			df_merged["pick_quantity"] = df_merged["pick_quantity"].apply(number_processing)
			df_merged["stock_for_put_away"] = df_merged["stock_for_put_away"].apply(number_processing)

			# data type
			df_merged["available_stock"] = pd.to_numeric(df_merged["available_stock"])
			df_merged["pick_quantity"] = pd.to_numeric(df_merged["pick_quantity"])
			df_merged["stock_for_put_away"] = pd.to_numeric(df_merged["stock_for_put_away"])
			df_merged["year_month_day"] = pd.to_datetime(df_merged["year_month_day"], format="%Y-%m-%d",
			                                             errors="coerce")

			return df_merged

	def vp_06_zprl_value(self):
		"""
        Data cleaning for pcl file from SAP virtual printer
        :return: DataFrame
        """

		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:
			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = (last_modified_time + datetime.timedelta(days=(-1))).date()

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"plant": [],
			                     "material_number": [],
			                     "warehouse_stock": [],
			                     "transfer_plant": [],
			                     "in_delivery": [],
			                     "segment": [],
			                     "sold_to_party": [],
			                     "name": [],
			                     "material_desc": [],
			                     "cont_grp_customer": [],
			                     "customer_material": [],
			                     "backlog_value": [],
			                     "material_type": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:14]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					dict_cleaned_data["plant"].append(value[0])
					dict_cleaned_data["material_number"].append(value[1])
					dict_cleaned_data["warehouse_stock"].append(value[11])
					dict_cleaned_data["transfer_plant"].append(value[3])
					dict_cleaned_data["in_delivery"].append(value[12])
					dict_cleaned_data["segment"].append(value[4])
					dict_cleaned_data["sold_to_party"].append(value[5])
					dict_cleaned_data["name"].append(value[6])
					dict_cleaned_data["material_desc"].append(value[7])
					dict_cleaned_data["cont_grp_customer"].append(value[8])
					dict_cleaned_data["customer_material"].append(value[9])
					dict_cleaned_data["backlog_value"].append(value[10])
					dict_cleaned_data["material_type"].append(value[2])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["material_number"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(inplace=True)

			# drop rows which material number length is less than 9
			length = df_merged["material_number"].astype(str).str.len()
			mask = length >= 9
			df_merged = df_merged[mask]

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# remove "," in numbers
			df_merged["backlog_value"] = df_merged["backlog_value"].apply(number_processing)

			# data type
			df_merged["backlog_value"] = pd.to_numeric(df_merged["backlog_value"])

			return df_merged

	def vp_07_zprl_qty(self):
		"""
        Data cleaning for pcl file from SAP virtual printer
        :return: DataFrame
        """

		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:
			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = (last_modified_time + datetime.timedelta(days=(-1))).date()

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"plant": [],
			                     "material_number": [],
			                     "warehouse_stock": [],
			                     "transfer_plant": [],
			                     "in_delivery": [],
			                     "segment": [],
			                     "sold_to_party": [],
			                     "name": [],
			                     "material_desc": [],
			                     "cont_grp_customer": [],
			                     "customer_material": [],
			                     "backlog_quantity": [],
			                     "material_type": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:14]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					dict_cleaned_data["plant"].append(value[0])
					dict_cleaned_data["material_number"].append(value[1])
					dict_cleaned_data["warehouse_stock"].append(value[11])
					dict_cleaned_data["transfer_plant"].append(value[3])
					dict_cleaned_data["in_delivery"].append(value[12])
					dict_cleaned_data["segment"].append(value[4])
					dict_cleaned_data["sold_to_party"].append(value[5])
					dict_cleaned_data["name"].append(value[6])
					dict_cleaned_data["material_desc"].append(value[7])
					dict_cleaned_data["cont_grp_customer"].append(value[8])
					dict_cleaned_data["customer_material"].append(value[9])
					dict_cleaned_data["backlog_quantity"].append(value[10])
					dict_cleaned_data["material_type"].append(value[2])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["material_number"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(inplace=True)

			# drop rows which material number length is less than 9
			length = df_merged["material_number"].astype(str).str.len()
			mask = length >= 9
			df_merged = df_merged[mask]

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# remove "," in numbers
			df_merged["backlog_quantity"] = df_merged["backlog_quantity"].apply(number_processing)

			# data type
			df_merged["backlog_quantity"] = pd.to_numeric(df_merged["backlog_quantity"])

			return df_merged

	def vp_08_zpcp13(self):
		"""
        Data cleaning for raw data.
        :return: DataFrame
        """

		# read data into pandas
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:
			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"MRP_controller": [],
			                     "MRP_group": [],
			                     "material_desc": [],
			                     "material_number": [],
			                     "days_supply": [],
			                     "category": [],
			                     "stock": [],
			                     "stock_p_plant": [],
			                     "backlog": [],
			                     "0": [],
			                     "1": [],
			                     "2": [],
			                     "3": [],
			                     "4": [],
			                     "5": [],
			                     "6": [],
			                     "7": [],
			                     "8": [],
			                     "9": [],
			                     "10": [],
			                     "11": [],
			                     "12": [],
			                     "total": [],
			                     "total_without_stock": [],
			                     "unit": [],
			                     "segment": [],
			                     "production_type": [],
			                     "production_type_desc": [],
			                     "MRP_lot_size": [],
			                     "min_lot_size": [],
			                     "plan_delivery_time": [],
			                     "safety_stock": [],
			                     "target_stock": [],
			                     "safety_time": [],
			                     "inhouse_production_time": [],
			                     "planning_time_fence": [],
			                     "purchase_group": [],
			                     "supplier": [],
			                     "plant_status": [],
			                     "stochastic_type": [],
			                     "ABC": [],
			                     "XYZ": [],
			                     "production_hierachy": [],
			                     "quality_inspection": [],
			                     "blocked": [],
			                     "configuable_material": [],
			                     "MRP_type": [],
			                     "LS": [],
			                     "target_stock_over": [],
			                     "stock_type": [],
			                     "round_value": [],
			                     "material_type": [],
			                     "material_group": [],
			                     "GR_processing_time": [],
			                     "annual_demand": [],
			                     "budget_year_requirement": [],
			                     "purchase_group_desc": [],
			                     "consumption_current_year_2": [],
			                     "consumption_current_year_1": [],
			                     "consumption_current_year": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:61]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["material_number"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(inplace=True)

			# rename column name
			first_date_string = data[2].split(sep="|")[10]
			first_date_string = first_date_string[-8:].strip()
			date_0 = datetime.datetime.strptime(first_date_string, "%m/%Y")

			# get next 12 months
			list_date = get_next_n_months(start_date=date_0, next_n=13)
			dict_column_mapping = {str(k): v for k, v in enumerate(list_date)}
			df_merged.rename(columns=dict_column_mapping, inplace=True)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# unpivot DataFrame
			list_fixed_column = []
			for i in df_merged.columns.to_list():
				if i not in dict_column_mapping.values():
					list_fixed_column.append(i)
			df_merged = df_merged.melt(id_vars=list_fixed_column,
			                           value_vars=list_date,
			                           var_name="date",
			                           value_name="quantity")

			# reset index
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["stock"] = df_merged["stock"].apply(number_processing)
			df_merged["backlog"] = df_merged["backlog"].apply(number_processing)
			df_merged["total"] = df_merged["total"].apply(number_processing)
			df_merged["total_without_stock"] = df_merged["total_without_stock"].apply(number_processing)
			df_merged["min_lot_size"] = df_merged["min_lot_size"].apply(number_processing)
			df_merged["target_stock"] = df_merged["target_stock"].apply(number_processing)
			df_merged["blocked"] = df_merged["blocked"].apply(number_processing)
			df_merged["round_value"] = df_merged["round_value"].apply(number_processing)
			df_merged["annual_demand"] = df_merged["annual_demand"].apply(number_processing)
			df_merged["budget_year_requirement"] = df_merged["budget_year_requirement"].apply(number_processing)
			df_merged["quantity"] = df_merged["quantity"].apply(number_processing)

			return df_merged

	def vp_09_pp_fauf(self):
		"""
        Data cleaning for raw data
        :return: DataFrame
        """
		# read data into pandas
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:
			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"oder_number": [],
			                     "order_type": [],
			                     "segment": [],
			                     "MRP_controller": [],
			                     "material_number": [],
			                     "material_desc": [],
			                     "basic_start_date": [],
			                     "basic_finish_date": [],
			                     "actual_finish_date": [],
			                     "order_quantity": [],
			                     "confirmed_quantity": [],
			                     "unit": [],
			                     "printed": [],
			                     "system_status_head": [],
			                     "confirm_enter_by": [],
			                     "confirm_date": [],
			                     "work_center": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:18]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["material_number"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# reset index
			df_merged.drop_duplicates(inplace=True)
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			return df_merged

	def vp_10_zco_mat_bewert(self):
		"""
        Data cleaning for raw data
        :return: DataFrame
        """

		# read data into pandas
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"plant": [],
			                     "segment": [],
			                     "material_number": [],
			                     "material_desc": [],
			                     "price_control": [],
			                     "valuation_class": [],
			                     "special_procurement": [],
			                     "price_unit": [],
			                     "GPC": [],
			                     "base_unit_of_measure": [],
			                     "moving_price": [],
			                     "standard_price": [],
			                     "GPC_date": [],
			                     "planned_price": [],
			                     "planned_price_date": []
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:16]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["material_number"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# reset index
			df_merged.drop_duplicates(inplace=True)
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["GPC_date"] = pd.to_datetime(df_merged["GPC_date"], format="%d.%m.%Y", errors="coerce")
			df_merged["price_unit"] = df_merged["price_unit"].apply(number_processing)
			df_merged["GPC"] = df_merged["GPC"].apply(number_processing)
			df_merged["moving_price"] = df_merged["moving_price"].apply(number_processing)
			df_merged["standard_price"] = df_merged["standard_price"].apply(number_processing)
			df_merged["planned_price"] = df_merged["planned_price"].apply(number_processing)
			df_merged["planned_price_date"] = pd.to_datetime(df_merged["planned_price_date"], format="%d.%m.%Y",
			                                                 errors="coerce")

			# data type
			df_merged["price_unit"] = pd.to_numeric(df_merged["price_unit"])
			df_merged["GPC"] = pd.to_numeric(df_merged["GPC"])
			df_merged["moving_price"] = pd.to_numeric(df_merged["moving_price"])
			df_merged["standard_price"] = pd.to_numeric(df_merged["standard_price"])
			df_merged["planned_price"] = pd.to_numeric(df_merged["planned_price"])

			return df_merged

	def vp_11_mm03(self):
		"""
        :return: DataFrame with cleaned data for "MM03"
        """

		# read data into pandas
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:
			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"plant": [],
			                     "segment": [],
			                     "material_number": [],
			                     "material_desc": [],
			                     "material_type": [],
			                     "assembly_scrap_ratio": [],
			                     "component_scrap_ratio": [],
			                     "prod_stor_location": [],
			                     "storage_loc_for_EP": [],
			                     "procurement_type": [],
			                     "special_procurement": [],
			                     "plant_sp_material_status": [],
			                     "price_control": [],
			                     "valuation_class": [],
			                     "MRP_controller": [],
			                     "prodn_supervisor": [],
			                     "backflush": [],
			                     "availability_check": [],
			                     "tot_repl_lead_time": [],
			                     "consumption_mode": [],
			                     "planning_strategy_group": [],
			                     "fwd_consumption_per": [],
			                     "bwd_consumption_per": [],
			                     "individual_coll": [],
			                     "selection_method": [],
			                     "GR_processing_time": [],
			                     "planned_delivery_time": [],
			                     "planning_time_fence": [],
			                     "in_house_production": [],
			                     "lot_size": [],
			                     "min_lot_size": [],
			                     "max_lot_size": [],
			                     "MRP_group": [],
			                     "MRP_type": [],
			                     "period_indicator": [],
			                     "safety_stock": [],
			                     "safety_time_ind": [],
			                     "safety_time_act_cov": [],
			                     "discontinuation_ind": [],
			                     "effective_out_date": [],
			                     "follow_up_material": [],
			                     "costing_lot_size": [],
			                     "budget_lot_size": [],
			                     "lab_office": [],
			                     "material_group": [],
			                     "planning_calendar": [],
			                     "product_hierarchy": [],
			                     "product_manager": [],
			                     "profit_center": [],
			                     "quota_arr_usage": [],
			                     "reorder_point": [],
			                     "spec_procurem_costing": [],
			                     "budget_flag_MRP": [],
			                     "budget_flag_calculation": [],
			                     "net_weight": [],
			                     "net_weight_unit": [],
			                     "weight_date": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:58]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["material_number"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(inplace=True)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["assembly_scrap_ratio"] = df_merged["assembly_scrap_ratio"].apply(number_processing)
			df_merged["component_scrap_ratio"] = df_merged["component_scrap_ratio"].apply(number_processing)
			df_merged["net_weight"] = df_merged["net_weight"].apply(number_processing)

			# convert data type
			df_merged["assembly_scrap_ratio"] = pd.to_numeric(df_merged["assembly_scrap_ratio"])
			df_merged["component_scrap_ratio"] = pd.to_numeric(df_merged["component_scrap_ratio"])
			df_merged["net_weight"] = pd.to_numeric(df_merged["net_weight"])
			df_merged["weight_date"] = pd.to_datetime(df_merged["weight_date"], format="%Y.%m.%d", errors="coerce")

			return df_merged

	def vp_12_eord_source_list(self):
		"""
        Data cleaning for raw data
        :return: DataFrame
        """

		# read data into pandas
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:
			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"client": [],
			                     "material_number": [],
			                     "plant": [],
			                     "number": [],
			                     "created_date": [],
			                     "created_by": [],
			                     "valid_from": [],
			                     "valid_to": [],
			                     "supplier": [],
			                     "fixed_vendor": [],
			                     "schedule_agreement": [],
			                     "item": [],
			                     "fixed_agreement_item": [],
			                     "procurement_plant": [],
			                     "fixed_issue_plant": [],
			                     "MPN_material": [],
			                     "blocked": [],
			                     "purchase_organization": [],
			                     "document_category": [],
			                     "source_category": [],
			                     "MRP": [],
			                     "order_unit": [],
			                     "logical_system": [],
			                     "special_stock": [],
			                     "central_contract": [],
			                     "central_contract_item": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[2:28]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["material_number"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# reset index
			df_merged.drop_duplicates(inplace=True)
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			return df_merged

	def vp_13_mb52_spare_parts(self):
		"""
        Data cleaning for raw data
        :return: DataFrame
        """

		# read data into pandas
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"material_number": [],
			                     "plant": [],
			                     "stock_location": [],
			                     "special_stock_category": [],
			                     "valuation": [],
			                     "special_stock_number": [],
			                     "stock_location_deletion_flag": [],
			                     "batch": [],
			                     "unit": [],
			                     "stock_unrestricted": [],
			                     "stock_segment": [],
			                     "currency": [],
			                     "value_unrestricted": [],
			                     "stock_in_transit": [],
			                     "value_in_transit": [],
			                     "stock_in_quality_inspection": [],
			                     "value_in_quality_inspection": [],
			                     "stock_restricted": [],
			                     "value_restricted": [],
			                     "stock_blocked": [],
			                     "value_blocked": [],
			                     "stock_returned": [],
			                     "value_returned": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:24]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["material_number"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# reset index
			df_merged.drop_duplicates(inplace=True)
			df_merged = df_merged[df_merged["material_number"] != "*"]
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["stock_unrestricted"] = df_merged["stock_unrestricted"].str.replace(",", "")
			df_merged["value_unrestricted"] = df_merged["value_unrestricted"].str.replace(",", "")
			df_merged["stock_in_transit"] = df_merged["stock_in_transit"].str.replace(",", "")
			df_merged["value_in_transit"] = df_merged["value_in_transit"].str.replace(",", "")
			df_merged["stock_in_quality_inspection"] = df_merged["stock_in_quality_inspection"].str.replace(",", "")
			df_merged["value_in_quality_inspection"] = df_merged["value_in_quality_inspection"].str.replace(",", "")
			df_merged["stock_restricted"] = df_merged["stock_restricted"].str.replace(",", "")
			df_merged["value_restricted"] = df_merged["value_restricted"].str.replace(",", "")
			df_merged["stock_blocked"] = df_merged["stock_blocked"].str.replace(",", "")
			df_merged["value_blocked"] = df_merged["value_blocked"].str.replace(",", "")
			df_merged["stock_returned"] = df_merged["stock_returned"].str.replace(",", "")
			df_merged["value_returned"] = df_merged["value_returned"].str.replace(",", "")

			return df_merged

	def vp_14_iw47_order_staff_time(self):
		"""
        Data cleaning for pcl files with
        :return: DataFrame
        """

		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get basic information for file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read file with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"order_type": [],
			                     "plant": [],
			                     "work_center": [],
			                     "activity_type": [],
			                     "order": [],
			                     "confirmation": [],
			                     "posting_date": [],
			                     "actual_start_date": [],
			                     "actual_start_time": [],
			                     "actual_finish_time": [],
			                     "employee_ID": [],
			                     "employee_name": [],
			                     "actual_work_minutes": [],
			                     "unit": [],
			                     "created_date": [],
			                     "created_by": [],
			                     "reverse": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:18]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["order"])
			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
			                       index=[i for i in range(0, row_count, 1)],
			                       columns=["file_name", "creation_time", "last_modified_time", "year_month",
			                                "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(keep="first", inplace=True)

			# remove invalid rows
			df_merged = df_merged[~df_merged["order"].isin(["*", ""])]

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["posting_date"] = pd.to_datetime(df_merged["posting_date"], format="%Y.%m.%d", errors="coerce")
			df_merged["actual_start_date"] = pd.to_datetime(df_merged["actual_start_date"], format="%Y.%m.%d",
			                                                errors="coerce")
			df_merged["created_date"] = pd.to_datetime(df_merged["created_date"], format="%Y.%m.%d", errors="coerce")
			df_merged["actual_work_minutes"] = df_merged["actual_work_minutes"].apply(lambda x: str(x).replace(",", ""))

			# convert data type
			df_merged["actual_work_minutes"] = pd.to_numeric(df_merged["actual_work_minutes"])

			return df_merged

	def vp_15_mb51_specified_mvt(self):
		"""
        Data cleaning for pcl files
        :return: DataFrame
        """

		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get basic information for file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read file with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"material_number": [],
			                     "plant": [],
			                     "name": [],
			                     "stock_location": [],
			                     "movement_type_desc": [],
			                     "movement_type": [],
			                     "supplier": [],
			                     "PO": [],
			                     "material_document": [],
			                     "batch": [],
			                     "posting_date": [],
			                     "quantity": [],
			                     "unit": [],
			                     "amount_LC": [],
			                     "user": [],
			                     "document_header_desc": [],
			                     "reference": [],
			                     "cost_center": [],
			                     "order": [],
			                     "material_desc": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|", maxsplit=20)[1:21]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame\
			row_count = len(dict_cleaned_data["material_number"])
			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
			                       index=[i for i in range(0, row_count, 1)],
			                       columns=["file_name", "creation_time", "last_modified_time", "year_month",
			                                "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(keep="first", inplace=True)

			# remove invalid rows
			df_merged = df_merged.drop(df_merged[df_merged["material_number"] == "*"].index)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["material_desc"] = df_merged["material_desc"].apply(lambda x: str(x)[:-1].strip())
			df_merged["posting_date"] = pd.to_datetime(df_merged["posting_date"], format="%Y.%m.%d", errors="coerce")
			df_merged["quantity"] = df_merged["quantity"].apply(number_processing)
			df_merged["amount_LC"] = df_merged["amount_LC"].apply(number_processing)

			# convert data type
			df_merged["quantity"] = pd.to_numeric(df_merged["quantity"], errors="coerce")
			df_merged["amount_LC"] = pd.to_numeric(df_merged["amount_LC"], errors="coerce")

			# 统一大小写
			df_merged["name"] = df_merged["name"].str.upper()
			df_merged["movement_type_desc"] = df_merged["movement_type_desc"].str.upper()
			df_merged["user"] = df_merged["user"].str.upper()
			df_merged["material_desc"] = df_merged["material_desc"].str.upper()

			return df_merged

	def vp_16_mm_pur_pr(self):
		"""
        Data cleaning for pcl files
        :return: DataFrame
        """

		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get basic information for file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read file with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"plant": [],
			                     "material_group": [],
			                     "material_number": [],
			                     "material_desc": [],
			                     "vendor_name": [],
			                     "PO": [],
			                     "PO_item": [],
			                     "order": [],
			                     "PR_item_quantity": [],
			                     "unit_1": [],
			                     "unit_2": [],
			                     "price_unit": [],
			                     "cost_center": [],
			                     "account_assignment_category": [],
			                     "GL_account": [],
			                     "PO_date": [],
			                     "currency": [],
			                     "PR": [],
			                     "PR_item": [],
			                     "receive_cost_center": [],
			                     "request_cost_center": [],
			                     "requested_by": [],
			                     "WBS_element": [],
			                     "purchase_document_type": [],
			                     "segment_number": [],
			                     "short_text": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|", maxsplit=26)[1:27]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["plant"])
			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
			                       index=[i for i in range(0, row_count, 1)],
			                       columns=["file_name", "creation_time", "last_modified_time", "year_month",
			                                "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(keep="first", inplace=True)

			# remove invalid rows
			df_merged = df_merged.drop(df_merged[df_merged["plant"] == "*"].index)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["PR_item_quantity"] = df_merged["PR_item_quantity"].apply(lambda x: str(x).replace(",", ""))
			df_merged["price_unit"] = df_merged["price_unit"].apply(lambda x: str(x).replace(",", ""))
			df_merged["PO_date"] = pd.to_datetime(df_merged["PO_date"], format="%Y.%m.%d", errors="coerce")
			df_merged["short_text"] = df_merged["short_text"].apply(lambda x: str(x)[:-1].strip())
			df_merged["PR_item"] = df_merged["PR_item"].apply(lambda x: str(x).rjust(5, "0"))
			df_merged["PO_item"] = df_merged["PO_item"].apply(lambda x: str(x).rjust(5, "0"))
			df_merged["segment_number"] = df_merged["segment_number"].apply(lambda x: str(x).rjust(6, "0"))

			# add columns
			df_merged["PR_type"] = df_merged.apply(determine_pr_po_type, axis=1)

			return df_merged

	def vp_17_coois_output(self):
		"""
        Data cleaning for raw data
        :return: DataFrame
        """

		# read data into pandas
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"plant": [],
			                     "order_number": [],
			                     "activity": [],
			                     "operation_quantity": [],
			                     "confirmation_number": [],
			                     "confirmation_counter": [],
			                     "posting_date": [],
			                     "shift": [],
			                     "work_center": [],
			                     "unit": [],
			                     "production_minute": [],
			                     "OK_quantity": [],
			                     "confirmation_minute": [],
			                     "rework_quantity": [],
			                     "scrap_quantity": [],
			                     "confirmation_type": [],
			                     "reversed_flag": [],
			                     "cancelled_confirmation_flag": [],
			                     "created_date": [],
			                     "Germany_time": [],
			                     "entered_by": [],
			                     "material_number": [],
			                     "material_desc": [],
			                     "machine_work_center": [],
			                     "Chinese_time": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:26]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["order_number"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# reset index
			df_merged.drop_duplicates(inplace=True)
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# convert data type
			df_merged["operation_quantity"] = df_merged["operation_quantity"].apply(number_processing)
			df_merged["posting_date"] = pd.to_datetime(df_merged["posting_date"], format="%Y.%m.%d")
			df_merged["production_minute"] = df_merged["production_minute"].apply(number_processing)
			df_merged["OK_quantity"] = df_merged["OK_quantity"].apply(number_processing)
			df_merged["confirmation_minute"] = df_merged["confirmation_minute"].apply(number_processing)
			df_merged["rework_quantity"] = df_merged["rework_quantity"].apply(number_processing)
			df_merged["scrap_quantity"] = df_merged["scrap_quantity"].apply(number_processing)
			df_merged["created_date"] = pd.to_datetime(df_merged["created_date"], format="%Y.%m.%d")

			# define data type
			df_merged["operation_quantity"] = pd.to_numeric(df_merged["operation_quantity"])
			df_merged["production_minute"] = pd.to_numeric(df_merged["production_minute"])
			df_merged["OK_quantity"] = pd.to_numeric(df_merged["OK_quantity"])
			df_merged["confirmation_minute"] = pd.to_numeric(df_merged["confirmation_minute"])
			df_merged["rework_quantity"] = pd.to_numeric(df_merged["rework_quantity"])
			df_merged["scrap_quantity"] = pd.to_numeric(df_merged["scrap_quantity"])

			return df_merged

	def vp_18_coois_order_routing(self):
		"""
        Data cleaning for raw data
        :return: DataFrame
        """

		# read data into pandas
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"order_number": [],
			                     "control_key": [],
			                     "activity": [],
			                     "setup_time_machine": [],
			                     "production_time_machine": [],
			                     "additional_time_machine": [],
			                     "number_staff_setup": [],
			                     "number_staff_production": [],
			                     "base_quantity": [],
			                     "work_center": [],
			                     "work_center_desc": [],
			                     "production_quantity": [],
			                     "yield_quantity": [],
			                     "scrap_quantity": [],
			                     "rework_quantity": [],
			                     "operation_start_date": [],
			                     "operation_start_time": [],
			                     "operation_finish_date": [],
			                     "operation_finish_time": [],
			                     "system_status": [],
			                     "equivalence_tool": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:-1]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["order_number"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# reset index
			df_merged.drop_duplicates(inplace=True)
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# convert data type
			df_merged["setup_time_machine"] = df_merged["setup_time_machine"].apply(number_processing)
			df_merged["production_time_machine"] = df_merged["production_time_machine"].apply(number_processing)
			df_merged["additional_time_machine"] = df_merged["additional_time_machine"].apply(number_processing)
			df_merged["number_staff_setup"] = df_merged["number_staff_setup"].apply(number_processing)
			df_merged["number_staff_production"] = df_merged["number_staff_production"].apply(number_processing)
			df_merged["base_quantity"] = df_merged["base_quantity"].apply(number_processing)
			df_merged["production_quantity"] = df_merged["production_quantity"].apply(number_processing)
			df_merged["yield_quantity"] = df_merged["yield_quantity"].apply(number_processing)
			df_merged["scrap_quantity"] = df_merged["scrap_quantity"].apply(number_processing)
			df_merged["rework_quantity"] = df_merged["rework_quantity"].apply(number_processing)
			df_merged["operation_start_date"] = pd.to_datetime(df_merged["operation_start_date"], format="%Y.%m.%d",
			                                                   errors="coerce")
			df_merged["operation_finish_date"] = pd.to_datetime(df_merged["operation_finish_date"], format="%Y.%m.%d",
			                                                    errors="coerce")

			# data type
			df_merged["setup_time_machine"] = pd.to_numeric(df_merged["setup_time_machine"])
			df_merged["production_time_machine"] = pd.to_numeric(df_merged["production_time_machine"])
			df_merged["additional_time_machine"] = pd.to_numeric(df_merged["additional_time_machine"])
			df_merged["number_staff_setup"] = pd.to_numeric(df_merged["number_staff_setup"])
			df_merged["number_staff_production"] = pd.to_numeric(df_merged["number_staff_production"])
			df_merged["base_quantity"] = pd.to_numeric(df_merged["base_quantity"])
			df_merged["production_quantity"] = pd.to_numeric(df_merged["production_quantity"])
			df_merged["yield_quantity"] = pd.to_numeric(df_merged["yield_quantity"])
			df_merged["scrap_quantity"] = pd.to_numeric(df_merged["scrap_quantity"])
			df_merged["rework_quantity"] = pd.to_numeric(df_merged["rework_quantity"])
			df_merged["equivalence_tool"] = pd.to_numeric(df_merged["equivalence_tool"])

			return df_merged

	def vp_19_se16_pp_qmeld(self):
		"""
        Data cleaning for raw data
        :return: DataFrame
        """

		# read data into pandas
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"client": [],
			                     "order_number": [],
			                     "sequence": [],
			                     "operation": [],
			                     "confirmation_counter": [],
			                     "posting_date": [],
			                     "work_center": [],
			                     "shift": [],
			                     "scrap_indicator": [],
			                     "quantity": [],
			                     "error_key": [],
			                     "burdening_plant": [],
			                     "burdening_cost_center": [],
			                     "burdening_work_center": [],
			                     "production_quantity": [],
			                     "operation_unit": [],
			                     "material_number": [],
			                     "plant": [],
			                     "cost_center": [],
			                     "work_center_category": [],
			                     "created_date": [],
			                     "created_time": [],
			                     "employee_ID": [],
			                     "down_time": [],
			                     "notification": [],
			                     "confirmation": [],
			                     "confirmation_counter_2": [],
			                     "type_of_message": [],
			                     "check_box": [],
			                     "deletion_flag": [],
			                     "change_document": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[2:33]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["employee_ID"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# reset index
			df_merged.drop_duplicates(inplace=True)
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# convert data type
			df_merged["posting_date"] = pd.to_datetime(df_merged["posting_date"], format="%Y.%m.%d", errors="coerce")
			df_merged["quantity"] = df_merged["quantity"].apply(number_processing)
			df_merged["production_quantity"] = df_merged["production_quantity"].apply(number_processing)
			df_merged["created_date"] = pd.to_datetime(df_merged["created_date"], format="%Y.%m.%d", errors="coerce")
			df_merged["down_time"] = df_merged["down_time"].apply(number_processing)

			# data type definition
			df_merged["quantity"] = pd.to_numeric(df_merged["quantity"])
			df_merged["production_quantity"] = pd.to_numeric(df_merged["production_quantity"])
			df_merged["down_time"] = pd.to_numeric(df_merged["down_time"])

			return df_merged

	def vp_20_se16_zpsollmin(self):
		"""
        Data cleaning for raw data
        :return: DataFrame
        """

		# read data into pandas
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"client": [],
			                     "plant": [],
			                     "cost_center": [],
			                     "work_center": [],
			                     "date": [],
			                     "shift": [],
			                     "target_minutes_shift_model": [],
			                     "target_minutes": [],
			                     "posting_minutes": [],
			                     "posting_date": [],
			                     "posting_time": [],
			                     "varient_shift": [],
			                     "KZ_Bedarfsma": [],
			                     "gepl_Einsatzmin": [],
			                     "KZ_U_Schicht": [],
			                     "pause_Enisatzzeit": [],
			                     "BUMIN_masch_stillst": [],
			                     "ist_einsatzmin": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[2:20]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["cost_center"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# reset index
			df_merged.drop_duplicates(inplace=True)
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# convert data type
			df_merged["date"] = pd.to_datetime(df_merged["date"], format="%Y.%m.%d", errors="coerce")
			df_merged["target_minutes_shift_model"] = df_merged["target_minutes_shift_model"].str.replace(",", "")
			df_merged["target_minutes"] = df_merged["target_minutes"].str.replace(",", "")
			df_merged["posting_minutes"] = df_merged["posting_minutes"].str.replace(",", "")
			df_merged["posting_date"] = pd.to_datetime(df_merged["posting_date"], format="%Y.%m.%d", errors="coerce")

			return df_merged

	def vp_21_se16_ekkn(self):
		"""
        Data cleaning for raw data
        :return: DataFrame
        """

		# read data into pandas
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"clint": [],
			                     "PO": [],
			                     "PO_item": [],
			                     "account_assignment_squence": [],
			                     "deletion_flag": [],
			                     "created_on": [],
			                     "change_flag": [],
			                     "quantity": [],
			                     "percent": [],
			                     "net_value": [],
			                     "GL_account": [],
			                     "business_area": [],
			                     "cost_center": [],
			                     "not_in_use": [],
			                     "SD_document": [],
			                     "item": [],
			                     "schedule_line_number": [],
			                     "gross_requirement_flag": [],
			                     "asset": [],
			                     "sub_number": [],
			                     "order": [],
			                     "recipient": [],
			                     "unloading_point": [],
			                     "controlling_area": [],
			                     "post_to_cost_center": [],
			                     "post_to_order": [],
			                     "post_to_project": [],
			                     "final_invoice": [],
			                     "cost_object": [],
			                     "segment_number": [],
			                     "profit_center": [],
			                     "WBS_element": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[2:34]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["PO"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# reset index
			df_merged.drop_duplicates(inplace=True)
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning for column
			df_merged["created_on"] = pd.to_datetime(df_merged["created_on"], format="%Y.%m.%d", errors="coerce")
			df_merged["net_value"] = df_merged["net_value"].apply(lambda x: str(x).replace(",", ""))

			return df_merged

	def vp_22_pp_fertvers_production_version(self):
		"""
        Data cleaning for pcl files
        :return: DataFrame
        """

		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get basic information for file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read file with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"plant": [],
			                     "segment": [],
			                     "material_number": [],
			                     "lock_flag": [],
			                     "short_name": [],
			                     "long_material_description": [],
			                     "version": [],
			                     "material_status": [],
			                     "product_hierarchy": [],
			                     "task_list_type": [],
			                     "group": [],
			                     "group_count": [],
			                     "routing_usage": [],
			                     "routing_satus": [],
			                     "alternative_bom": [],
			                     "bom_status": [],
			                     "bom_usage": [],
			                     "text": [],
			                     "valid_from": [],
			                     "valid_to": [],
			                     "selection_method": [],
			                     "material_description": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:23]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["material_number"])
			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
			                       index=[i for i in range(0, row_count, 1)],
			                       columns=["file_name", "creation_time", "last_modified_time", "year_month",
			                                "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(keep="first", inplace=True)

			# remove invalid rows
			df_merged.dropna(subset=["material_number"], axis=0, inplace=True)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["valid_from"] = pd.to_datetime(df_merged["valid_from"], format="%Y.%m.%d", errors="coerce")
			# df_merged["valid_to"] = pd.to_datetime(df_merged["valid_to"], format="%Y.%m.%d", errors="coerce")

			return df_merged

	def vp_23_y_ed1_27000648(self):
		"""
        Data cleaning for raw data
        :return: DataFrame
        """

		# define variables to save key string
		str_title = "Plant Cost Report"

		# mapping profit center and segment profit center
		pattern = r":\s*([A-Za-z0-9-]+)"
		dict_profit_center = {
			"8101-S20": "8101-S20",
			"8101-S22": "8101-S22",
			"8101-S36": "8101-S36",
			"8101-S30": "8101-S30",
			"8101-S31": "8101-S31",
			"8101-S33": "8101-S33",
			"1515-PCR_N": "1515-PCR_N"
		}
		dir_garbage = r"\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\08_Private Database\99_python_application\Virtual_Printer\23_Y_ED1_27000648\Garbage"
		dir_done = r"\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\08_Private Database\99_python_application\Virtual_Printer\23_Y_ED1_27000648\Done"

		# get basic information of file
		file_path = Path(self.dir_pcl).stat()
		file_name = Path(self.dir_pcl).stem
		creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
		last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)

		# read data into pandas
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# read data with Python
			data = f.readlines()

			# get profit center and segment profit center
			profit_center = re.search(pattern=pattern, string=data[4]).group(1)
			segment_profit_center = re.search(pattern=pattern, string=data[6]).group(1)

			# if content in the file can meet following conditions
			df_merged = pd.DataFrame()
			if (str_title in data[1]) and (profit_center == segment_profit_center):

				# get date
				year_month_day = None
				year_month = None
				for i in data[1].split(sep=" "):
					try:

						# get original date
						original_date = datetime.datetime.strptime(i, "%d.%m.%Y")

						# get previous date of original date
						year_month_day = original_date - datetime.timedelta(days=1)
						year_month = datetime.datetime.strftime(year_month_day, "%Y%m")
					except:
						pass

				# get profit center
				profit_center = None
				for j in data[4].split(sep=" "):
					if j in dict_profit_center.keys():
						profit_center = j

				# define dictionary to save raw data
				dict_cleaned_data = {"cost_category": [],
				                     "plan_cost_in_CNY": [],
				                     "actual_cost_in_CNY": [],
				                     "deviation_in_CNY": [],
				                     "deviation_percentage": [],
				                     "plan_YTD_cost_in_CNY": [],
				                     "actual_YTD_cost_in_CNY": [],
				                     "YTD_deviation_in_CNY": [],
				                     "YTD_deviation_percentage": [],
				                     }

				for row in data:

					if row.count("|") >= 10:

						# split string
						value = row.split(sep="|")[1:10]

						# remove white space in string
						value = [i.strip() for i in value]

						# append data into dictionary
						for t in enumerate(dict_cleaned_data.keys()):
							column_position = t[0]
							column_name = t[1]

							dict_cleaned_data[column_name].append(value[column_position])

				# create DataFrame
				row_count = len(dict_cleaned_data["cost_category"])

				df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
				df_file = pd.DataFrame(
						[[file_name, creation_time, last_modified_time, year_month, year_month_day, profit_center]],
						index=[i for i in range(0, row_count, 1)],
						columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day",
						         "profit_center"])

				# concatenate DataFrame
				df_merged = pd.concat([df_file, df_data], axis=1)

				# reset index
				df_merged.drop_duplicates(inplace=True)
				df_merged.dropna(subset=["cost_category"], how="any", axis=0, inplace=True)
				df_merged.reset_index(drop=True, inplace=True)
				df_merged.drop(index=[0], inplace=True)
				df_merged.reset_index(drop=True, inplace=True)

				# data cleaning
				df_merged["cost_category"] = df_merged["cost_category"].str.replace("*", "").str.strip()
				df_merged["plan_cost_in_CNY"] = df_merged["plan_cost_in_CNY"].apply(number_processing)
				df_merged["actual_cost_in_CNY"] = df_merged["actual_cost_in_CNY"].apply(number_processing)
				df_merged["deviation_in_CNY"] = df_merged["deviation_in_CNY"].apply(number_processing)
				df_merged["plan_YTD_cost_in_CNY"] = df_merged["plan_YTD_cost_in_CNY"].apply(number_processing)
				df_merged["actual_YTD_cost_in_CNY"] = df_merged["actual_YTD_cost_in_CNY"].apply(number_processing)
				df_merged["YTD_deviation_in_CNY"] = df_merged["YTD_deviation_in_CNY"].apply(number_processing)

				# drop blank rows
				df_merged = df_merged[df_merged["cost_category"] != ""]

				# data type
				df_merged["plan_cost_in_CNY"] = pd.to_numeric(df_merged["plan_cost_in_CNY"])
				df_merged["actual_cost_in_CNY"] = pd.to_numeric(df_merged["actual_cost_in_CNY"])
				df_merged["deviation_in_CNY"] = pd.to_numeric(df_merged["deviation_in_CNY"])
				df_merged["plan_YTD_cost_in_CNY"] = pd.to_numeric(df_merged["plan_YTD_cost_in_CNY"])
				df_merged["actual_YTD_cost_in_CNY"] = pd.to_numeric(df_merged["actual_YTD_cost_in_CNY"])
				df_merged["YTD_deviation_in_CNY"] = pd.to_numeric(df_merged["YTD_deviation_in_CNY"])

				# move file to "Done"
				f.close()
				shutil.move(src=self.dir_pcl, dst=Path(dir_done).joinpath(Path(self.dir_pcl).name))

			# move file to "Garbage"
			else:
				# close file
				f.close()

				# move file
				dir_dst = Path.joinpath(Path(dir_garbage), Path(self.dir_pcl).name)
				shutil.move(src=self.dir_pcl, dst=dir_dst)

			return df_merged

	def vp_24_coois_header(self):
		"""
        Data cleaning for pcl files
        :return: DataFrame
        """

		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get basic information for file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read file with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"order": [],
			                     "order_type": [],
			                     "material_number": [],
			                     "material_desc": [],
			                     "alternative_BOM": [],
			                     "explosion_date": [],
			                     "MRP_controller": [],
			                     "production_supervisor": [],
			                     "target_quantity": [],
			                     "delivery_quantity": [],
			                     "confirmed_quantity": [],
			                     "scrap_quantity": [],
			                     "rework_quantity": [],
			                     "unit": [],
			                     "basic_start_date": [],
			                     "basic_finish_date": [],
			                     "actual_finish_date": [],
			                     "system_status": [],
			                     "routing_group_counter": []
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:20]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["order"])
			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
			                       index=[i for i in range(0, row_count, 1)],
			                       columns=["file_name", "creation_time", "last_modified_time", "year_month",
			                                "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(keep="first", inplace=True)

			# remove invalid rows
			df_merged.dropna(subset=["order"], axis=0, inplace=True)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["explosion_date"] = pd.to_datetime(df_merged["explosion_date"], format="%Y.%m.%d",
			                                             errors="coerce")
			df_merged["target_quantity"] = df_merged["target_quantity"].apply(number_processing)
			df_merged["delivery_quantity"] = df_merged["delivery_quantity"].apply(number_processing)
			df_merged["confirmed_quantity"] = df_merged["confirmed_quantity"].apply(number_processing)
			df_merged["scrap_quantity"] = df_merged["scrap_quantity"].apply(number_processing)
			df_merged["rework_quantity"] = df_merged["rework_quantity"].apply(number_processing)
			df_merged["basic_start_date"] = pd.to_datetime(df_merged["basic_start_date"], format="%Y.%m.%d",
			                                               errors="coerce")
			df_merged["basic_finish_date"] = pd.to_datetime(df_merged["basic_finish_date"], format="%Y.%m.%d",
			                                                errors="coerce")
			df_merged["actual_finish_date"] = pd.to_datetime(df_merged["actual_finish_date"], format="%Y.%m.%d",
			                                                 errors="coerce")

			# data type
			df_merged["target_quantity"] = pd.to_numeric(df_merged["target_quantity"])
			df_merged["delivery_quantity"] = pd.to_numeric(df_merged["delivery_quantity"])
			df_merged["confirmed_quantity"] = pd.to_numeric(df_merged["confirmed_quantity"])
			df_merged["scrap_quantity"] = pd.to_numeric(df_merged["scrap_quantity"])
			df_merged["rework_quantity"] = pd.to_numeric(df_merged["rework_quantity"])

			return df_merged

	def vp_25_me5a(self):
		"""
        Data cleaning for pcl files
        :return: DataFrame
        """

		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get basic information for file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read file with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"purchased_organization": [],
			                     "plant": [],
			                     "purchase_group": [],
			                     "S": [],
			                     "document_type": [],
			                     "PR": [],
			                     "PR_item": [],
			                     "PO": [],
			                     "material_number": [],
			                     "MRP_controller": [],
			                     "stock_location": [],
			                     "delivery_date_category": [],
			                     "delivery_date": [],
			                     "fixed_vendor": [],
			                     "vendor_name": [],
			                     "supplier_plant": [],
			                     "requested_by": [],
			                     "quantity": [],
			                     "unit": [],
			                     "creation_flag": [],
			                     "overall_release": [],
			                     "release_date": [],
			                     "unit_price": [],
			                     "price_unit": [],
			                     "total_value": [],
			                     "currency": [],
			                     "created_by": [],
			                     "requisition_date": [],
			                     "changed_date": [],
			                     "vendor_material_number": [],
			                     "consumption": [],
			                     "PO_date": [],
			                     "customer": [],
			                     "vendor": [],
			                     "manufacturer": [],
			                     "external_manufacturer": [],
			                     "deletion_flag": [],
			                     "release_flag": [],
			                     "PR_item_desc": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:40]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["PR"])
			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
			                       index=[i for i in range(0, row_count, 1)],
			                       columns=["file_name", "creation_time", "last_modified_time", "year_month",
			                                "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(keep="first", inplace=True)

			# remove invalid rows
			df_merged.dropna(subset=["PR"], axis=0, inplace=True)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["PR_item_desc"] = df_merged["PR_item_desc"].apply(lambda x: str(x)[:-1].strip())
			df_merged["PR_item"] = df_merged["PR_item"].apply(lambda x: str(x).rjust(5, "0"))
			df_merged["delivery_date"] = pd.to_datetime(df_merged["delivery_date"], format="%Y%m%d", errors="coerce")
			df_merged["quantity"] = df_merged["quantity"].apply(lambda x: str(x).replace(",", ""))
			df_merged["release_date"] = pd.to_datetime(df_merged["release_date"], format="%Y.%m.%d", errors="coerce")
			df_merged["unit_price"] = df_merged["unit_price"].apply(lambda x: str(x).replace(",", ""))
			df_merged["price_unit"] = df_merged["price_unit"].apply(lambda x: str(x).replace(",", ""))
			df_merged["total_value"] = df_merged["total_value"].apply(lambda x: str(x).replace(",", ""))
			df_merged["requisition_date"] = pd.to_datetime(df_merged["requisition_date"], format="%Y.%m.%d",
			                                               errors="coerce")
			df_merged["changed_date"] = pd.to_datetime(df_merged["changed_date"], format="%Y.%m.%d",
			                                           errors="coerce")
			df_merged["PO_date"] = pd.to_datetime(df_merged["PO_date"], format="%Y.%m.%d",
			                                      errors="coerce")

			return df_merged

	def vp_26_ebkn(self):
		"""
        Data cleaning for pcl files
        :return: DataFrame
        """

		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get basic information for file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read file with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"client": [],
			                     "PR": [],
			                     "PR_item": [],
			                     "account_assignment": [],
			                     "deletion_flag": [],
			                     "created_date": [],
			                     "created_by": [],
			                     "requested_quantity": [],
			                     "percentage": [],
			                     "GL_account": [],
			                     "business_area": [],
			                     "cost_center": [],
			                     "not_in_use": [],
			                     "SD_document": [],
			                     "SD_document_item": [],
			                     "schedule_line_number": [],
			                     "asset": [],
			                     "sub_number": [],
			                     "order": [],
			                     "recipient": [],
			                     "unloading_point": [],
			                     "controlling_area": [],
			                     "posting_to_cost_center": [],
			                     "posting_to_order": [],
			                     "posting_to_project": [],
			                     "cost_object": [],
			                     "profit_segment": [],
			                     "profit_center": [],
			                     "WBS_element": [],
			                     "network": [],
			                     "routing_number_for_operations_1": [],
			                     "real_estate_key": [],
			                     "counter_1": [],
			                     "partner": [],
			                     "commitment_item": [],
			                     "recovery_flag": [],
			                     "request_cost_center": [],
			                     "transport_cost_center": [],
			                     "receiving_cost_center": [],
			                     "follow_up_cost_center": [],
			                     "requested_by": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[2:43]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["PR"])
			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
			                       index=[i for i in range(0, row_count, 1)],
			                       columns=["file_name", "creation_time", "last_modified_time", "year_month",
			                                "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(keep="first", inplace=True)

			# remove invalid rows
			df_merged.dropna(subset=["PR"], axis=0, inplace=True)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["requested_quantity"] = df_merged["requested_quantity"].apply(lambda x: str(x).replace(",", ""))

			return df_merged

	def vp_27_iw39(self):
		"""
        Data cleaning for pcl files
        :return: DataFrame
        """

		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get basic information for file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read file with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"selected_line": [],
			                     "order_type": [],
			                     "maintenance_type": [],
			                     "long_text_exist": [],
			                     "priority_text": [],
			                     "order": [],
			                     "description": [],
			                     "user_status": [],
			                     "equipment": [],
			                     "object_description": [],
			                     "plant_section": [],
			                     "basic_start_date": [],
			                     "basic_finish_date": [],
			                     "plant": [],
			                     "main_work_center": [],
			                     "currency": [],
			                     "actual_total_cost": [],
			                     "plan_total_cost": [],
			                     "estimated_cost": [],
			                     "created_on": [],
			                     "entered_by  ": [],
			                     "changed_on": [],
			                     "changed_by": [],
			                     "responsible_cost_center": [],
			                     "system_status": [],
			                     "message": [],
			                     "profit_center": [],
			                     "cost_center": []
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:29]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["order"])
			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
			                       index=[i for i in range(0, row_count, 1)],
			                       columns=["file_name", "creation_time", "last_modified_time", "year_month",
			                                "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(keep="first", inplace=True)

			# remove invalid rows
			df_merged.dropna(subset=["order"], axis=0, inplace=True)
			df_merged = df_merged[df_merged["order"].apply(lambda x: len(x) >= 9)]

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["basic_start_date"] = pd.to_datetime(df_merged["basic_start_date"], format="%Y.%m.%d",
			                                               errors="coerce")
			df_merged["basic_finish_date"] = pd.to_datetime(df_merged["basic_finish_date"], format="%Y.%m.%d",
			                                                errors="coerce")
			df_merged["actual_total_cost"] = df_merged["actual_total_cost"].apply(lambda x: str(x).replace(",", ""))
			df_merged["plan_total_cost"] = df_merged["plan_total_cost"].apply(lambda x: str(x).replace(",", ""))
			df_merged["estimated_cost"] = df_merged["estimated_cost"].apply(lambda x: str(x).replace(",", ""))
			df_merged["created_on"] = pd.to_datetime(df_merged["created_on"], format="%Y.%m.%d", errors="coerce")
			df_merged["changed_on"] = pd.to_datetime(df_merged["changed_on"], format="%Y.%m.%d", errors="coerce")

			return df_merged

	def vp_28_mm_pur_po(self):
		"""
        Data cleaning for pcl files
        :return: DataFrame
        """

		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get basic information for file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read file with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"plant": [],
			                     "country": [],
			                     "material_group": [],
			                     "material_number": [],
			                     "material_desc": [],
			                     "supplier": [],
			                     "vendor_name": [],
			                     "valid_start": [],
			                     "valid_end": [],
			                     "next_FRC": [],
			                     "PO": [],
			                     "PO_item": [],
			                     "PDT_EKPO": [],
			                     "profile": [],
			                     "firm_zone": [],
			                     "trade_off_zone": [],
			                     "confirmation_control_key": [],
			                     "order": [],
			                     "PO_quantity": [],
			                     "order_price_unit": [],
			                     "order_unit": [],
			                     "price_unit": [],
			                     "net_price": [],
			                     "cost_center": [],
			                     "account_type": [],
			                     "posting_date": [],
			                     "GL_account": [],
			                     "GR_IR_flag": [],
			                     "PO_date": [],
			                     "movement_type": [],
			                     "currency": [],
			                     "item_category": [],
			                     "material_document": [],
			                     "PR": [],
			                     "PR_item": [],
			                     "received_cost_center": [],
			                     "requested_cost_center": [],
			                     "requested_by": [],
			                     "requisitioner": [],
			                     "WBS_element": [],
			                     "delivery_date": [],
			                     "PO_type": [],
			                     "GR_quantity": [],
			                     "delivery_complete_flag": [],
			                     "order_confirmation": [],
			                     "PO_item_desc": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|", maxsplit=46)[1:47]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["PO"])
			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
			                       index=[i for i in range(0, row_count, 1)],
			                       columns=["file_name", "creation_time", "last_modified_time", "year_month",
			                                "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(keep="first", inplace=True)

			# remove invalid rows
			df_merged = df_merged.drop(df_merged[df_merged["plant"] == "*"].index)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["PO_quantity"] = df_merged["PO_quantity"].apply(lambda x: str(x).replace(",", ""))
			df_merged["net_price"] = df_merged["net_price"].apply(lambda x: str(x).replace(",", ""))
			df_merged["posting_date"] = pd.to_datetime(df_merged["posting_date"], format="%Y.%m.%d", errors="coerce")
			df_merged["PO_date"] = pd.to_datetime(df_merged["PO_date"], format="%Y.%m.%d", errors="coerce")
			df_merged["valid_start"] = pd.to_datetime(df_merged["valid_start"], format="%Y.%m.%d", errors="coerce")
			df_merged["valid_end"] = pd.to_datetime(df_merged["valid_end"], format="%Y.%m.%d", errors="coerce")
			df_merged["delivery_date"] = pd.to_datetime(df_merged["delivery_date"], format="%Y.%m.%d", errors="coerce")
			df_merged["GR_quantity"] = df_merged["GR_quantity"].apply(lambda x: str(x).replace(",", ""))
			df_merged["price_unit"] = df_merged["price_unit"].apply(lambda x: str(x).replace(",", ""))
			df_merged["PO_item_desc"] = df_merged["PO_item_desc"].apply(lambda x: str(x)[:-1].strip())
			df_merged["PO_item"] = df_merged["PO_item"].apply(lambda x: str(x).rjust(5, "0"))
			df_merged["PR_item"] = df_merged["PR_item"].apply(lambda x: str(x).rjust(5, "0"))

			# add columns
			df_merged["PR_type"] = df_merged.apply(determine_pr_po_type, axis=1)

			# convert data type
			df_merged["PO_quantity"] = pd.to_numeric(df_merged["PO_quantity"], errors="coerce")
			df_merged["price_unit"] = pd.to_numeric(df_merged["price_unit"], errors="coerce")
			df_merged["net_price"] = pd.to_numeric(df_merged["net_price"], errors="coerce")
			df_merged["GR_quantity"] = pd.to_numeric(df_merged["GR_quantity"], errors="coerce")

			return df_merged

	def vp_29_csks(self):
		"""
        Data cleaning for raw data
        :return: DataFrame
        """

		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get basic information for file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read file with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"client": [],
			                     "controlling_area": [],
			                     "CC_TCC": [],
			                     "valid_to": [],
			                     "valid_from": [],
			                     "actual_primary": [],
			                     "plan_primary": [],
			                     "company_code": [],
			                     "business_area": [],
			                     "CC_category": [],
			                     "responsible": [],
			                     "responsible_user": [],
			                     "currency": [],
			                     "costing_sheet": [],
			                     "tax": [],
			                     "profit_center": [],
			                     "plant": [],
			                     "logical_system": [],
			                     "created_on": [],
			                     "created_by": [],
			                     "actual_secondary": [],
			                     "actual_revenue": [],
			                     "commitment": [],
			                     "plan_secondary": [],
			                     "plan_revenue": [],
			                     "allocation_method": [],
			                     "record_quantity": [],
			                     "department": [],
			                     "sub_sequence_CC": [],
			                     "usage": [],
			                     "application": [],
			                     "overhead_key": [],
			                     "country": [],
			                     "title": [],
			                     "name": [],
			                     "name_1": [],
			                     "name_2": [],
			                     "name_3": [],
			                     "city": [],
			                     "district": [],
			                     "street": [],
			                     "po_box": [],
			                     "postal_code": [],
			                     "po_bos_code": [],
			                     "region": [],
			                     "language": [],
			                     "telebox": [],
			                     "telephone_1": [],
			                     "telephone_2": [],
			                     "fax_number": [],
			                     "teletex": [],
			                     "telex": [],
			                     "data_line": [],
			                     "printer_name": [],
			                     "hierarchy": [],
			                     "cost_collector": [],
			                     "complete": [],
			                     "statistics_falg": [],
			                     "object_number": [],
			                     "function": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[2:62]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["CC_TCC"])
			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
			                       index=[i for i in range(0, row_count, 1)],
			                       columns=["file_name", "creation_time", "last_modified_time", "year_month",
			                                "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(keep="first", inplace=True)

			# remove invalid rows
			df_merged.dropna(subset=["CC_TCC"], axis=0, inplace=True)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["responsible"] = df_merged["responsible"].str.upper()
			df_merged["valid_to"] = df_merged["valid_to"].apply(lambda s: datetime.datetime.strptime(s, "%Y.%m.%d"))
			df_merged["valid_from"] = df_merged["valid_from"].apply(lambda s: datetime.datetime.strptime(s, "%Y.%m.%d"))
			df_merged["created_on"] = pd.to_datetime(df_merged["created_on"], format="%Y.%m.%d",
			                                         errors="coerce")

			df_merged["CC_TCC_category"] = df_merged["CC_TCC"].apply(lambda x: "CC" if len(str(x)) == 9 else "TCC")

			# add columns
			df_merged["actual_block"] = df_merged.apply(lambda row: "blocked" if row["actual_primary"] == "X" and row[
				"actual_secondary"] == "X" else "unblocked", axis=1)
			df_merged["plan_block"] = df_merged.apply(lambda row: "blocked" if row["plan_primary"] == "X" and row[
				"plan_secondary"] == "X" else "unblocked", axis=1)

			return df_merged

	def vp_30_work_center(self):
		"""
        Data cleaning for raw data
        :return: DataFrame
        """

		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get basic information for file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read file with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"plant": [],
			                     "segment": [],
			                     "work_center": [],
			                     "short_description": [],
			                     "TCC": [],
			                     "level": [],
			                     "lower_level": [],
			                     "category": [],
			                     "capa_quantity": [],
			                     "is_deleted": [],
			                     "is_locked": [],
			                     "control_key": [],
			                     "subsystem": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:14]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["work_center"])
			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
			                       index=[i for i in range(0, row_count, 1)],
			                       columns=["file_name", "creation_time", "last_modified_time", "year_month",
			                                "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(keep="first", inplace=True)

			# remove invalid rows
			df_merged.dropna(subset=["work_center"], axis=0, inplace=True)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["EWC_MWC"] = df_merged["work_center"].apply(
					lambda x: "EWC" if str(x).startswith("SCT") else "MWC")

			return df_merged

	def vp_31_ih08(self):
		"""
        Data cleaning for raw data
        :return: DataFrame
        """

		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get basic information for file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read file with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"S": [],
			                     "equipment": [],
			                     "serial_number": [],
			                     "equipment_category": [],
			                     "planning_plant": [],
			                     "maintenance_plant": [],
			                     "technical_object_desc": [],
			                     "plant_section": [],
			                     "planner_group": [],
			                     "main_work_center": [],
			                     "cost_center": [],
			                     "super_equipment": [],
			                     "function_location": [],
			                     "ABC_flag": [],
			                     "inventory_number": [],
			                     "manufacturer_asset": [],
			                     "construction_year": [],
			                     "manufacturer_serial_number": [],
			                     "room": [],
			                     "technical_identification_number": [],
			                     "sort_field": [],
			                     "asset_number": [],
			                     "sub_asset_number": [],
			                     "object_type": [],
			                     "system_status": [],
			                     "user_status": [],
			                     "created_on": [],
			                     "created_by": [],
			                     "changed_on": [],
			                     "changed_by": [],
			                     "location": [],
			                     "construction_type": [],
			                     "material_number": [],
			                     "material_desc": [],
			                     "plant": [],
			                     "stock_location": [],
			                     "external_calibration": [],
			                     "model_number": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:39]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["equipment"])
			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
			                       index=[i for i in range(0, row_count, 1)],
			                       columns=["file_name", "creation_time", "last_modified_time", "year_month",
			                                "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(keep="first", inplace=True)

			# remove invalid rows
			df_merged.dropna(subset=["equipment"], axis=0, inplace=True)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["created_on"] = pd.to_datetime(df_merged["created_on"], format="%Y.%m.%d", errors="coerce")
			df_merged["changed_on"] = pd.to_datetime(df_merged["changed_on"], format="%Y.%m.%d", errors="coerce")

			# filter data in Plant 3
			# df_profit_center = profit_center()
			# list_segment_number = df_profit_center["segment_number"].unique().tolist()
			# df_merged = df_merged[df_merged["plant_section"].isin([i[0:3] for i in list_segment_number])]

			return df_merged

	def vp_32_ekko(self):
		"""
        Data cleaning for raw data
        :return: DataFrame
        """

		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get basic information for file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read file with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"client": [],
			                     "PO": [],
			                     "company_code": [],
			                     "document_category": [],
			                     "document_type": [],
			                     "control": [],
			                     "delete_flas": [],
			                     "status": [],
			                     "created_on": [],
			                     "created_by": [],
			                     "item_interval": [],
			                     "last_item": [],
			                     "supplier_code": [],
			                     "language": [],
			                     "payment_terms": [],
			                     "payment_in_1": [],
			                     "payment_in_2": [],
			                     "payment_in_3": [],
			                     "disc_percent_1": [],
			                     "disc_percent_2": [],
			                     "purchase_organization": [],
			                     "purchase_group": [],
			                     "currency": [],
			                     "exchange_rate": [],
			                     "exchange_rate_fixed": [],
			                     "document_date": [],
			                     "valid_start": [],
			                     "valid_end": [],
			                     "application_by": [],
			                     "quotation_deadline": [],
			                     "binding_period": [],
			                     "warranty": [],
			                     "bid_invitation": [],
			                     "quotation": [],
			                     "quotation_date": [],
			                     "your_reference": [],
			                     "sales_person": [],
			                     "telephone": [],
			                     "vendor": [],
			                     "customer": [],
			                     "agreement": [],
			                     "field_not_used": [],
			                     "complete_delivery": [],
			                     "GR_message": [],
			                     "supply_plant": [],
			                     "receive_vendor": [],
			                     "incoterms": [],
			                     "incoterms_2": [],
			                     "target_value": [],
			                     "collective_number": [],
			                     "document_condition": [],
			                     "procedure": [],
			                     "update_group": [],
			                     "invoice_party": [],
			                     "foreign_trade_data_number": [],
			                     "our_reference": [],
			                     "logical_system": [],
			                     "subitem_interval": [],
			                     "time_dep_condition": [],
			                     "release_group": [],
			                     "release_strategy": [],
			                     "release_flag": [],
			                     "release_status": [],
			                     "subject_to_release": [],
			                     "report_country": [],
			                     "release_document": [],
			                     "address_number": [],
			                     "country_tax_number": [],
			                     "vat_reg_number": [],
			                     "reason_for_cancellation": [],
			                     "document_number": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[2:73]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["PO"])
			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
			                       index=[i for i in range(0, row_count, 1)],
			                       columns=["file_name", "creation_time", "last_modified_time", "year_month",
			                                "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(keep="first", inplace=True)

			# remove invalid rows
			df_merged.dropna(subset=["PO"], axis=0, inplace=True)
			df_merged = df_merged[df_merged["PO"].apply(lambda x: len(x) >= 7)]

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["created_on"] = pd.to_datetime(df_merged["created_on"], format="%Y.%m.%d",
			                                         errors="coerce")
			df_merged["document_date"] = pd.to_datetime(df_merged["document_date"], format="%Y.%m.%d",
			                                            errors="coerce")
			df_merged["valid_start"] = pd.to_datetime(df_merged["valid_start"], format="%Y.%m.%d", errors="coerce")
			df_merged["valid_end"] = pd.to_datetime(df_merged["valid_end"], format="%Y.%m.%d", errors="coerce")

			return df_merged

	def vp_33_routing_pp_aplan(self):
		"""
        Data cleaning for pcl files
        :return: DataFrame
        """

		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get basic information for file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])
			routing_export_date = datetime.datetime.strftime(last_modified_time, "%Y%m%d")
			routing_reversion = last_modified_time.date()
			plant = "8101"

			# read file with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"MRP_controller": [],
			                     "plant_sp_material_status": [],
			                     "group": [],
			                     "group_counter": [],
			                     "segment_number": [],
			                     "MRP_type": [],
			                     "material_number": [],
			                     "material_description": [],
			                     "assembly_scrap (%)": [],
			                     "component_scrap (%)": [],
			                     "operation": [],
			                     "control_key": [],
			                     "work_center": [],
			                     "work_center_name": [],
			                     "number_splits": [],
			                     "costing_lot_size": [],
			                     "budget_lot_size": [],
			                     "operation_desc": [],
			                     "conversion_header": [],
			                     "conversion_operation": [],
			                     "base_quantity": [],
			                     "base_unit": [],
			                     "setup_time_machine": [],
			                     "setup_time_machine_unit": [],
			                     "activity_setup_time_machine": [],
			                     "production_time_machine": [],
			                     "production_time_machine_unit": [],
			                     "activity_production_time_machine": [],
			                     "sec_add_time_m": [],
			                     "sec_add_time_m_unit": [],
			                     "number_staff_setup": [],
			                     "number_staff_setup_unit": [],
			                     "activity_type_number_staff_setup": [],
			                     "number_staff_production": [],
			                     "unit_of_meas_for_stand_Val": [],
			                     "activity_number_stand_Val": [],
			                     "equivalence_tool": [],
			                     "equivalence_tool_unit": [],
			                     "activity_type_equivalence_tool": [],
			                     "valid_from": [],
			                     "usage": [],
			                     "status": [],
			                     "shop_box_rout_slip": [],
			                     "deletion_indicator": [],
			                     "delind_TL": [],
			                     "relevancy_to_costing_indicator": [],
			                     "required_splitting": [],
			                     "setup_group_category": [],
			                     "setup_group_key": [],
			                     "calculation_type": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:51]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["material_number"])
			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, routing_export_date, routing_reversion, plant]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "routing_export_date",
					         "routing_reversion", "plant"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(keep="first", inplace=True)

			# remove invalid rows
			df_merged.dropna(subset=["material_number"], axis=0, inplace=True)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["valid_from"] = pd.to_datetime(df_merged["valid_from"], format="%Y.%m.%d", errors="coerce")
			df_merged["costing_lot_size"] = df_merged["costing_lot_size"].apply(lambda x: str(x).replace(",", ""))
			df_merged["budget_lot_size"] = df_merged["budget_lot_size"].apply(lambda x: str(x).replace(",", ""))
			df_merged["base_quantity"] = df_merged["base_quantity"].apply(lambda x: str(x).replace(",", ""))

			return df_merged

	def vp_34_bom_pp_stueli(self):
		"""
        Data cleaning for pcl files
        :return: DataFrame
        """

		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get basic information for file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read file with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"plant": [],
			                     "assembly_plant": [],
			                     "assembly_material": [],
			                     "assembly_description": [],
			                     "assembly_material_status": [],
			                     "assembly_procurement_type": [],
			                     "assembly_special_procurement_type": [],
			                     "assembly_material_type": [],
			                     "item_category": [],
			                     "alternative_group": [],
			                     "component": [],
			                     "component_description": [],
			                     "component_qty": [],
			                     "component_unit": [],
			                     "base_quantity": [],
			                     "Item_number": [],
			                     "bom_number": [],
			                     "component_material_type": [],
			                     "cost_relevant": [],
			                     "bom_usage": [],
			                     "alternative_bom": [],
			                     "usage_probability": [],
			                     "bom_status": [],
			                     "document": [],
			                     "document_part": [],
			                     "document_type": [],
			                     "version": [],
			                     "change_number": [],
			                     "component_scrap": [],
			                     "distribution_key": [],
			                     "component_procurement_type": [],
			                     "component_special_procurement_type": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:33]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["assembly_material"])
			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
			                       index=[i for i in range(0, row_count, 1)],
			                       columns=["file_name", "creation_time", "last_modified_time", "year_month",
			                                "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(keep="first", inplace=True)

			# remove invalid rows
			df_merged.dropna(subset=["assembly_material"], axis=0, inplace=True)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["component_qty"] = df_merged["component_qty"].apply(number_processing)
			df_merged["base_quantity"] = df_merged["base_quantity"].apply(number_processing)

			# convert data type
			df_merged["component_qty"] = pd.to_numeric(df_merged["component_qty"])
			df_merged["base_quantity"] = pd.to_numeric(df_merged["base_quantity"])

			return df_merged

	def vp_35_bom_pp_stueli_future(self):
		"""
        Data cleaning for pcl files
        :return: DataFrame
        """

		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:

			# get basic information for file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read file with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"plant": [],
			                     "assembly_plant": [],
			                     "assembly_material": [],
			                     "assembly_description": [],
			                     "assembly_material_status": [],
			                     "assembly_procurement_type": [],
			                     "assembly_special_procurement_type": [],
			                     "assembly_material_type": [],
			                     "item_category": [],
			                     "alternative_group": [],
			                     "component": [],
			                     "component_description": [],
			                     "component_qty": [],
			                     "component_unit": [],
			                     "base_quantity": [],
			                     "Item_number": [],
			                     "bom_number": [],
			                     "component_material_type": [],
			                     "cost_relevant": [],
			                     "bom_usage": [],
			                     "alternative_bom": [],
			                     "usage_probability": [],
			                     "bom_status": [],
			                     "document": [],
			                     "document_part": [],
			                     "document_type": [],
			                     "version": [],
			                     "change_number": [],
			                     "component_scrap": [],
			                     "distribution_key": [],
			                     "component_procurement_type": [],
			                     "component_special_procurement_type": [],
			                     "bom_valid_from": [],
			                     "operation_scrap": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:35]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["assembly_material"])
			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame([[file_name, creation_time, last_modified_time, year_month, year_month_day]],
			                       index=[i for i in range(0, row_count, 1)],
			                       columns=["file_name", "creation_time", "last_modified_time", "year_month",
			                                "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(keep="first", inplace=True)

			# remove invalid rows
			df_merged.dropna(subset=["assembly_material"], axis=0, inplace=True)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["component_qty"] = df_merged["component_qty"].apply(lambda x: str(x).replace(",", ""))
			df_merged["base_quantity"] = df_merged["base_quantity"].apply(lambda x: str(x).replace(",", ""))
			df_merged["bom_valid_from"] = pd.to_datetime(df_merged["bom_valid_from"], format="%Y.%m.%d",
			                                             errors="coerce")

			return df_merged

	def vp_36_mm03_whole_8101(self):
		"""
        :return: DataFrame with cleaned data for "MM03"
        """

		# read data into pandas
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:
			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"plant": [],
			                     "segment": [],
			                     "material_number": [],
			                     "material_desc": [],
			                     "material_type": [],
			                     "assembly_scrap_ratio": [],
			                     "component_scrap_ratio": [],
			                     "prod_stor_location": [],
			                     "storage_loc_for_EP": [],
			                     "procurement_type": [],
			                     "special_procurement": [],
			                     "plant_sp_material_status": [],
			                     "price_control": [],
			                     "valuation_class": [],
			                     "MRP_controller": [],
			                     "prodn_supervisor": [],
			                     "backflush": [],
			                     "availability_check": [],
			                     "tot_repl_lead_time": [],
			                     "consumption_mode": [],
			                     "planning_strategy_group": [],
			                     "fwd_consumption_per": [],
			                     "bwd_consumption_per": [],
			                     "individual_coll": [],
			                     "selection_method": [],
			                     "GR_processing_time": [],
			                     "planned_delivery_time": [],
			                     "planning_time_fence": [],
			                     "in_house_production": [],
			                     "lot_size": [],
			                     "min_lot_size": [],
			                     "max_lot_size": [],
			                     "MRP_group": [],
			                     "MRP_type": [],
			                     "period_indicator": [],
			                     "safety_stock": [],
			                     "safety_time_ind": [],
			                     "safety_time_act_cov": [],
			                     "discontinuation_ind": [],
			                     "effective_out_date": [],
			                     "follow_up_material": [],
			                     "costing_lot_size": [],
			                     "budget_lot_size": [],
			                     "lab_office": [],
			                     "material_group": [],
			                     "planning_calendar": [],
			                     "product_hierarchy": [],
			                     "product_manager": [],
			                     "profit_center": [],
			                     "quota_arr_usage": [],
			                     "reorder_point": [],
			                     "spec_procurem_costing": [],
			                     "budget_flag_MRP": [],
			                     "budget_flag_calculation": [],
			                     "net_weight": [],
			                     "net_weight_unit": [],
			                     "automatic_PO": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:58]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["material_number"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(inplace=True)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			return df_merged

	def vp_37_cskt(self):
		"""
        :return: DataFrame with cleaned data for "MM03"
        """

		# read data into pandas
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:
			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"client": [],
			                     "language": [],
			                     "controlling_area": [],
			                     "CC_TCC": [],
			                     "valid_to": [],
			                     "Function": [],
			                     "CC_TCC_desc": [],
			                     "CC_TCC_short_text": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[2:10]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["CC_TCC"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(inplace=True)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["valid_to"] = df_merged["valid_to"].apply(lambda s: datetime.datetime.strptime(s, "%Y.%m.%d"))

			return df_merged

	def vp_38_zpseg_kst(self):
		"""
        :return: DataFrame with cleaned data for "MM03"
        """

		# read data into pandas
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:
			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"client": [],
			                     "controlling_area": [],
			                     "cost_center": [],
			                     "changed_date": [],
			                     "changed_time": [],
			                     "valid_from": [],
			                     "segment": [],
			                     "sub_segment": [],
			                     "group_of_types": [],
			                     "indicator": [],
			                     "changed_by": [],
			                     "plant": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[2:14]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["cost_center"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(inplace=True)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["changed_date"] = pd.to_datetime(df_merged["changed_date"], format="%Y.%m.%d", errors="coerce")
			df_merged["valid_from"] = pd.to_datetime(df_merged["valid_from"], format="%Y.%m.%d", errors="coerce")

			return df_merged

	def vp_39_dn_monitor_zlsvn(self):
		"""
        :return: DataFrame with cleaned data for "MM03"
        """

		# 定义占位字符串
		str_occupied = "aa"

		# read data into pandas
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:
			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"delivery_number": [],
			                     "material_number": [],
			                     "created_by": [],
			                     "created_on": [],
			                     "created_time": [],
			                     "ship_to": [],
			                     "shipment": [],
			                     "ship_to_party_name": []
			                     }

			for row in data:

				# remove first "|" and last "|"
				row = row[1:-1]

				try:

					# split string
					value = row.split(sep=" ")

					# remove white space in string
					value = [i.strip() for i in value if i != ''][1:]

					# check valid rows
					if value[0][0].isdigit():

						# 若第2个元素不是数字开头，则在第7号位置插入1个占位字符串
						if not value[1][0].isdigit():
							value.insert(1, str_occupied)

						# 若第7个元素不是数字开头，则在第7号位置插入1个占位字符串
						if not value[6][0].isdigit():
							value.insert(6, str_occupied)

						part_1 = value[0:7]
						part_2 = " ".join(value[7:])

						# 清洗part_2
						if part_2.endswith("|"):
							part_2 = part_2[:-1].strip()

						# 追加至行末尾
						part_1.append(part_2)

						# 数据追加至字典
						for t in enumerate(dict_cleaned_data.keys()):
							column_position = t[0]
							column_name = t[1]

							dict_cleaned_data[column_name].append(part_1[column_position])

				except:
					pass

			# create DataFrame
			row_count = len(dict_cleaned_data["delivery_number"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(inplace=True)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["created_on"] = pd.to_datetime(df_merged["created_on"], format="%Y.%m.%d", errors="coerce")
			df_merged["shipment"] = df_merged["shipment"].apply(lambda x: None if str(x) == str_occupied else x)
			df_merged["material_number"] = df_merged["material_number"].apply(
					lambda x: None if str(x) == str_occupied else x)

			return df_merged

	def vp_40_coois_order_component(self):
		"""
        :return: DataFrame with cleaned data for "MM03"
        """

		# read data into pandas
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:
			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"order": [],
			                     "order_header": [],
			                     "item_category": [],
			                     "material_number": [],
			                     "bom_item": [],
			                     "requirement_date": [],
			                     "requirement_quantity": [],
			                     "quantity_withdrawn": [],
			                     "unit": [],
			                     "backflush_flag": [],
			                     "alternative_bom": [],
			                     "status": [],
			                     "work_center": [],
			                     "activity": [],
			                     "material_desc": []
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:16]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["order"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(inplace=True)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["requirement_date"] = pd.to_datetime(df_merged["requirement_date"], format="%Y.%m.%d",
			                                               errors="coerce")
			df_merged["requirement_quantity"] = df_merged["requirement_quantity"].apply(number_processing)
			df_merged["quantity_withdrawn"] = df_merged["quantity_withdrawn"].apply(number_processing)

			return df_merged

	def vp_41_se16_draw(self):
		"""
        :return: DataFrame with cleaned data for "SE16_DRAW"
        """

		# read data into pandas
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:
			# get creation time of file
			file_path = Path(self.dir_pcl).stat()
			file_name = Path(self.dir_pcl).stem
			creation_time = datetime.datetime.fromtimestamp(file_path.st_ctime)
			last_modified_time = datetime.datetime.fromtimestamp(file_path.st_mtime)
			year_month_day = last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(last_modified_time.year), str(last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"client": [],
			                     "document_type": [],
			                     "document_number": [],
			                     "document_version": [],
			                     "document_part": [],
			                     "user_name": [],
			                     "document_status": [],
			                     "lab_office": [],
			                     "change_number": [],
			                     "plant": [],
			                     "deletion_flag": [],
			                     "CAD_flag": [],
			                     "sup_document": [],
			                     "sup_document_version": [],
			                     "sup_document_part": [],
			                     "sup_document_type": [],
			                     "authority_group": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[2:19]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["document_number"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[file_name, creation_time, last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(inplace=True)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			return df_merged

	def vp_42_zpseg(self):
		"""
        :return: DataFrame with cleaned data for "SE16_DRAW"
        """

		# read data into pandas
		with open(file=self.dir_pcl, mode="r", encoding="GBK") as f:
			# get creation time of file
			year_month_day = self.last_modified_time.strftime("%Y-%m-%d")
			year_month = "".join([str(self.last_modified_time.year), str(self.last_modified_time.month).rjust(2, "0")])

			# read data with Python
			data = f.readlines()

			# define dictionary to save raw data
			dict_cleaned_data = {"client": [],
			                     "plant": [],
			                     "segment": [],
			                     "segment_1": [],
			                     "segment_2": [],
			                     "segment_3": [],
			                     "segment_description": [],
			                     "responsible": [],
			                     "profit_center": [],
			                     "profit_center_new": [],
			                     "delete_date": [],
			                     }

			for row in data:

				try:

					# split string
					value = row.split(sep="|")[1:12]

					# remove white space in string
					value = [i.strip() for i in value]

					# append data into dictionary
					for t in enumerate(dict_cleaned_data.keys()):
						column_position = t[0]
						column_name = t[1]

						dict_cleaned_data[column_name].append(value[column_position])

				except Exception as e:
					print(f"{data.index(row)} : {row} \n {e}")

			# create DataFrame
			row_count = len(dict_cleaned_data["segment"])

			df_data = pd.DataFrame(dict_cleaned_data, index=[i for i in range(0, row_count, 1)])
			df_file = pd.DataFrame(
					[[self.file_name, self.creation_time, self.last_modified_time, year_month, year_month_day]],
					index=[i for i in range(0, row_count, 1)],
					columns=["file_name", "creation_time", "last_modified_time", "year_month", "year_month_day"])

			# concatenate DataFrame
			df_merged = pd.concat([df_file, df_data], axis=1)

			# drop duplicated rows
			df_merged.drop_duplicates(inplace=True)
			df_merged.dropna(subset=["profit_center", "segment", "segment_1", "segment_2", "segment_3"], how="any",
			                 axis=0, inplace=True)

			# reset index
			df_merged.reset_index(drop=True, inplace=True)
			df_merged.drop(index=[0], inplace=True)
			df_merged.reset_index(drop=True, inplace=True)

			# data cleaning
			df_merged["client"] = df_merged["client"].apply(lambda s: str(s).split(sep=" ")[-1].strip())
			df_merged["segment_number"] = pd.concat(
					[df_merged["segment"], df_merged["segment_1"], df_merged["segment_2"], df_merged["segment_3"]],
					axis=1).apply(
					lambda x: "".join(x.astype(str)), axis=1)
			df_merged["delete_date"] = pd.to_datetime(df_merged["delete_date"], format="%Y.%m.%d", errors="coerce")

			return df_merged
