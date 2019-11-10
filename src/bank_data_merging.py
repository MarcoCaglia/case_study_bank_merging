import json
import os
import re
import pandas as pd
from sqlalchemy import create_engine


class NonMatchingShapesError(Exception):
    """Exception raised, when number of columns are different in loaded files.
    """


class FormatRuleError(Exception):
    """Exception raised, when column data type can not be converted to
    requested types.
    """


class BankNotFoundError(Exception):
    """Exception raised, when source bank is not found in meta dictionary, and
    no default entry is given.
    """


class UnknownDataTypeError(Exception):
    """Exception raised, when requested export datatype is not available.
    """


class BankMerge():
    """Class to conveniently handle merging of several csv files. Refer to
    methods for more details.

    Raises:
        BankNotFoundError: Exception raised, when source bank is not found in
                           meta dictionary, and no default entry is given.
        UnknownDataTypeError: Exception raised, when requested export datatype
                              is not available.
        FormatRuleError: Exception raised, when column data type can not be
                         converted to requested types.
        NonMatchingShapesError: Exception raised, when number of columns are
                                different in loaded files.
    """

    def __init__(self, file_path=None, ignore_shape=False):
        """Initialization of Bank Merge Class.

        Keyword Arguments:
            file_path {string} -- Directory, where source data is located.
                                  Can be set to none if source data is located
                                  current working directory. (default: None)
            ignore_shape {bool} -- Wheter to ignore the shape of the source
                                   data. If set to False, it will be asserted,
                                   that the source files have the same number
                                   of columns  (default: False)
        """
        self.bank_files = self._get_files(file_path, ignore_shape)

    def merge(self,
              custom_meta_dict=None,
              check_types=True,
              run_dry=False,
              export_meta_dict_path=None):
        """Merge loaded files into single dataframe. Harmonizes columns
        before merging process.

        Keyword Arguments:
            custom_meta_dict {string} -- Directory of json file with
                                        harmonization information. If no
                                        directory is given, default
                                        harmonization will be used
                                        (default: None).
            check_types {bool} -- Check if column datatypes contain
                                requested datatypes. Can be used as
                                sanity check for unreliable data
                                (default: True)
            run_dry {bool} -- If true, no action will be executed, except
                            for the export of the loaded harmonization
                            information, if a path has been specified
                            (default: False).
            export_meta_dict_path {string} -- Directory to export
                                            harmonization information
                                            to (default: None).
        """
        if custom_meta_dict:
            with open(custom_meta_dict, 'r') as f:
                meta_dict = json.load(f)
        else:
            meta_dict = self._get_default_meta_dict()
        if export_meta_dict_path:
            with open(export_meta_dict_path, 'w') as f:
                json.dump(export_meta_dict_path, f)
        if not run_dry:
            harmonized = []
            for bank in self.bank_files:
                if bank in meta_dict.keys():
                    harmonized.append(self._harmonize(self.bank_files[bank],
                                                      meta_dict[bank],
                                                      check_types
                                                      )
                                      )
                elif 'default' in meta_dict.keys():
                    harmonized.append(self._harmonize(self.bank_files[bank],
                                                      meta_dict['default'],
                                                      check_types
                                                      )
                                      )
                else:
                    raise BankNotFoundError("""{} not found in meta dictionary.
                                               Please add bank to meta
                                               dictionary orset a default
                                               entry.""".format(bank))

            self.merged_data = pd.concat(harmonized, sort=False) \
                                 .reset_index(drop=True)

    def export(self, name='merged_data', output_type='csv', output_path=None):
        """Exports previously merged data to on of several possible output
        types.

        Keyword Arguments:
            name {str} -- Desired name of output (default: {'merged_data'}).
            output_type {str} -- Desired output type. Possible are .csv, .json,
                                 and .xml. For export to DB choose the method
                                 export_to_sql (default: {'csv'}).
            output_path {[type]} -- Desired output path. If none is given, file
                                    will be output to current working directory
                                    (default: {None}).
        """
        if not output_path:
            output_path = os.getcwd()
        if output_type == 'csv':
            self.merged_data.to_csv(output_path+'/'+name, index=False)
        elif output_type == 'json':
            self.merged_data.to_json(output_path+'/'+name)
        elif output_type == 'xml':
            xml_series = self.merged_data.apply(self._make_xml, axis=1)
            xml_string = ''
            for string in xml_series:
                xml_string = xml_string + string
            with open(output_path+'/'+name, 'w') as f:
                f.write(xml_string)
        elif output_type == 'sql':
            raise UnknownDataTypeError(
                """To export to a SQL Database, please use the export_to_sql
                   method.""")
        else:
            raise UnknownDataTypeError("""Output type {} is unknown. Please use
                                          one of the following: csv, json, xml.
            """.format(output_type))

    def _make_xml(self, row_in_df):
        xml = ['<item>']
        for col in row_in_df.index:
            xml.append(
                '  <field name="{0}">{1}</field>'.format(col, row_in_df[col]))
        xml.append('</item>')
        return '\n'.join(xml)

    def export_to_sql(self, engine_url, **kwargs):
        """Exports previously merged data to SQL DB. Keyword arguments are
        passed to pandas to_sql().

        Arguments:
            engine_url {string} -- DB URL for sqlalchemy.
        """
        engine = create_engine(engine_url)
        self.merged_data.to_sql(con=engine, **kwargs)

    def _harmonize(self, bank_df, col_dict, check_types):
        harmonized_df = bank_df.copy()
        for key, value in col_dict.items():
            for col in harmonized_df.columns:
                if col in value[0]:
                    harmonized_df.rename(columns={col: key}, inplace=True)
                    try:
                        if value[1][0] == 'datetime' and check_types:
                            harmonized_df[key] = pd.to_datetime(
                                harmonized_df[key],
                                format=value[1][1],
                                errors='raise')
                        elif value[1][0] == 'object' and check_types:
                            harmonized_df[key] = harmonized_df[key].astype(
                                'str')
                        elif value[1][0] == 'numeric' and check_types:
                            pd.to_datetime(harmonized_df[key], errors='raise')
                    except FormatRuleError():
                        print("""Column {} could not beconverted into requested
                                 format. Check input data
                                 or set check_types to False to
                                 ignore formats.""".format(key))

        return harmonized_df

    def _get_default_meta_dict(self):
        meta_dict = {
            'default': {
                'date': (['date', 'dates', 'datetime'], ('datetime', None)),
                'type': (['type', 'transaction'], ('object')),
                'amount': (['amount', 'amounts'], ('numeric')),
                'to': (['to'], 'numeric'),
                'from': (['from'], 'numeric')
            }
        }

        return meta_dict

    def _get_files(self, file_path, ignore_shape):
        if file_path:
            save_path = os.getcwd()
            os.chdir(file_path)
        all_names = os.listdir()
        match_pattern = re.compile(r'.*\.csv')
        frames = {}
        for name in all_names:
            new_name = re.findall(match_pattern, name)
            if len(new_name) != 0:
                frames[new_name[0].replace('.csv', '')] = pd.read_csv(name)

        if not ignore_shape:
            try:
                n_cols = list(frames.values())[0].shape[1]
                for frame in frames.values():
                    assert frame.shape[1] == n_cols
            except NonMatchingShapesError():
                print("""Input with different numbers of columns detected.
                         Check input or set ignore_shape to True to ignore.""")
        if file_path:
            os.chdir(save_path)

        return frames
