from __future__ import absolute_import
import os
import copy
import numpy as np

from . import translations
from ...util import proxy_object
from six.moves import range
from six.moves import zip


class HaloStatFile(object):
    """Manages and reads a halo stat file of unspecified format."""
    _finder_offset_start = 0 #whether finder_offset should start at 0 (default) or N (typically either 0 or 1)
    _column_translations = {}

    def __new__(cls, timestep):
        subcls = cls.find_loadable_subclass(timestep)
        if subcls:
            return object.__new__(subcls)
        else:
            raise IOError("No stat file found for timestep %r"%timestep)

    @classmethod
    def find_loadable_subclass(cls, timestep_filename):
        if cls.can_load(timestep_filename):
            return cls
        for subclass in cls.__subclasses__():
            loadable_cls = subclass.find_loadable_subclass(timestep_filename)
            if loadable_cls:
                return loadable_cls
        return None

    @classmethod
    def can_load(cls, timestep_filename):
        try:
            return os.path.exists(cls.filename(timestep_filename))
        except (ValueError, TypeError):
            return False

    @classmethod
    def filename(cls, timestep_filename):
        raise ValueError("Unknown path to stat file")

    def __init__(self, timestep_filename):
        self._timestep_filename = timestep_filename
        self.filename = self.filename(timestep_filename)

    def all_columns(self):
        with open(self.filename) as f:
            columns = self._read_column_names(f)

        columns+=self._column_translations.keys()
        return columns

    def iter_rows_raw(self, *args):
        """
        Yield
        1) the index in which each halo appears in the catalog (starting from 0 unless _finder_offset_start is set)
        2) the raw halo ID (finder_id) included in the halo stat file without any emulation
        3) the rest of the requested parameters

        :param args: strings for the column names
        :return: finder_offset_start, finder_id, arg1, arg2, arg3, ... where finder_offset_start is the index of the halo within
        the stat file, finder_id is the raw halo ID read from the stat file, and argN is the value associated with the
        Nth column name provided as input.
        """
        with open(self.filename) as f:
            header = self._read_column_names(f)
            cnt = 0
            ids = [0]
            for a in args:
                try:
                    ids.append(header.index(a))
                except ValueError:
                    ids.append(None)
            for l in f:
                if not l.startswith("#"):
                    col_data = self._get_values_for_columns(ids, l)
                    col_data.insert(0, cnt+self._finder_offset_start)
                    yield col_data
                    cnt += 1

    def iter_rows(self, *args):
        """
        Yield the requested column values from the halo catalog stat file, as well as the finder_offset (index associated
        with halo's position within the catalog) and finder_id (raw halo id listed in the stat file).

        Returned halo properties are emulated when necessary. For example, AHF stat files do not contain n_dm; however,
        its value can be automatically inferred by this function. Meanwhile IDL .amiga.stat files rename n_gas as N_gas.

        :param args: strings for the column names
        :return: finder_offset, finder_id, arg1, arg2, arg3 where argN is the value of the Nth named column
        """

        raw_args = []
        for arg in args:
            if arg in self._column_translations:
                raw_args+=self._column_translations[arg].inputs()
            else:
                raw_args.append(arg)
        for raw_values in self.iter_rows_raw(*raw_args):
            values = [raw_values[0], raw_values[1]]
            for arg in args:
                if arg in self._column_translations:
                    values.append(self._column_translations[arg](raw_args, raw_values[2:]))
                else:
                    values.append(raw_values[2:][raw_args.index(arg)])
            yield values

    def read(self, *args):
        """Read the halo ID and requested columns from the entire file, returning each column as a separate array"""
        return_values = [[] for _ in range(len(args)+2)]
        for row in self.iter_rows(*args):
            for return_array, value in zip(return_values, row):
                return_array.append(value)

        return [np.array(x) for x in return_values]

    def _get_values_for_columns(self, columns, line):
        results = []
        l_split = line.split()
        for id_this in columns:
            if id_this is None:
                this_cast = None
            else:
                this_str = l_split[id_this]
                if "." in this_str or "e" in this_str:
                    guess_type = float
                else:
                    guess_type = int

                try:
                    this_cast = guess_type(this_str)
                except ValueError:
                    this_cast = this_str

            results.append(this_cast)
        return results

    def _read_column_names(self, f):
        return [x.split("(")[0] for x in f.readline().split()]




class AHFStatFile(HaloStatFile):
    _finder_offset_start = 1

    _column_translations = {'n_gas': translations.DefaultValue('n_gas', 0),
                            'n_star': translations.DefaultValue('n_star', 0),
                            'n_dm': translations.Function(lambda ngas, nstar, npart: npart - (ngas or 0) - (nstar or 0),
                                                          'n_gas', 'n_star', 'npart'),
                            'hostHalo': translations.Function(
                                lambda id: None if id==-1 else proxy_object.IncompleteProxyObjectFromFinderId(id, 'halo'),
                                'hostHalo')}

    def __init__(self, timestep_filename):
        super(AHFStatFile, self).__init__(timestep_filename)
        self._column_translations = copy.copy(self._column_translations)
        self._column_translations['childHalo'] = translations.Function(self._child_halo_entry, '#ID')

    @classmethod
    def filename(cls, timestep_filename):
        import glob
        file_list = glob.glob(timestep_filename+ '.z*.???.AHF_halos')

        # permit the AHF halos to be in a subfolder called "halos", for yt purposes
        # (where the yt tipsy reader can't cope with the AHF files being in the same folder)
        parts = timestep_filename.split("/")
        parts_with_halo = parts[:-1]+["halos"]+parts[-1:]
        filename_with_halo = "/".join(parts_with_halo)
        file_list+=glob.glob(filename_with_halo+'.z*.???.AHF_halos')

        if len(file_list)==0:
            return "CannotFindAHFHaloFilename"
        else:
            return file_list[0]

    def _calculate_children(self):
        # use hostHalo column to calculate virtual childHalo entries
        self._children_map = {}
        for c_id, f_id, host_f_id in self.iter_rows_raw("hostHalo"):
            if host_f_id!=-1:
                cmap = self._children_map.get(host_f_id, [])
                cmap.append(proxy_object.IncompleteProxyObjectFromFinderId(f_id,'halo'))
                self._children_map[host_f_id] = cmap

    def _calculate_children_if_required(self):
        if not hasattr(self, "_children_map"):
            self._calculate_children()

    def _child_halo_entry(self, this_id_raw):
        self._calculate_children_if_required()
        children = self._children_map.get(this_id_raw, [])
        return children

class RockstarStatFile(HaloStatFile):
    _column_translations = {'n_dm': translations.Rename('Np'),
                            'n_gas': translations.Value(0),
                            'n_star': translations.Value(0),
                            'npart': translations.Rename('Np')}

    @classmethod
    def filename(cls, timestep_filename):
        basename = os.path.basename(timestep_filename)
        dirname = os.path.dirname(timestep_filename)
        if basename.startswith("snapshot_"):
            timestep_id = int(basename[9:])
            return os.path.join(dirname, "out_%d.list"%timestep_id)
        else:
            return "CannotComputeRockstarFilename"

class AmigaIDLStatFile(HaloStatFile):

    _column_translations = {'n_dm': translations.Rename('N_dark'),
                            'n_gas': translations.Rename('N_gas'),
                            'n_star': translations.Rename("N_star"),
                            'npart': translations.Function(lambda ngas, nstar, ndark: ngas + nstar + ndark,
                                                           "N_dark", "N_gas", "N_star")}

    @classmethod
    def filename(cls, timestep_filename):
        return timestep_filename + '.amiga.stat'

    def iter_rows_raw(self, *args):
        """
        Yield the halo ID along with the values associated with each of the given arguments. The halo ID is output twice
        in order to be consistent with other stat file readers. In this case, the finder_offset that is normally output
        is just equal to the finder_id.

        :param args: strings for the column names
        :return: finder_id, finder_id, arg1, arg2, arg3, ... where finder_id is the halo's ID number read directly
        from the stat file and argN is the value associated with the Nth column name given as arguments.
        """

        for row in super().iter_rows_raw(*args):
            row[0] = row[1]  # sequential catalog index not right in this case; overwrite to match finder id
            yield row
