from __future__ import absolute_import
import os

import numpy as np

from . import translations
from six.moves import range
from six.moves import zip


class HaloStatFile(object):
    """Manages and reads a halo stat file of unspecified format."""
    _id_offset = 0
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
        Yield the halo ID and requested columns from each line of the stat file, without any emulation.

        :param args: strings for the column names
        :return: id, arg1, arg2, arg3 where ID is the halo ID and argN is the value of the Nth named column
        """
        with open(self.filename) as f:
            header = self._read_column_names(f)
            ids = [0] + [header.index(a) for a in args]
            for l in f:
                if not l.startswith("#"):
                    yield self._get_values_for_columns(ids, l)

    def iter_rows(self, *args):
        """
        Yield the halo ID and requested columns from each line of the stat file, emulating the existence of some columns.

        For example, AHF stat files do not contain n_dm; however, its value can be automatically inferred by this
        function. Meanwhile IDL .amiga.stat files rename n_gas as N_gas.

        :param args: strings for the column names
        :return: id, arg1, arg2, arg3 where ID is the halo ID and argN is the value of the Nth named column
        """

        raw_args = []
        for arg in args:
            if arg in self._column_translations:
                raw_args+=self._column_translations[arg].inputs()
            else:
                raw_args.append(arg)

        for raw_values in self.iter_rows_raw(*raw_args):
            values = [raw_values[0]]
            for arg in args:
                if arg in self._column_translations:
                    values.append(self._column_translations[arg](raw_args, raw_values[1:]))
                else:
                    values.append(raw_values[1:][raw_args.index(arg)])
            yield values

    def read(self, *args):
        """Read the halo ID and requested columns from the entire file, returning each column as a separate array"""
        return_values = [[] for _ in range(len(args)+1)]
        for row in self.iter_rows(*args):
            for return_array, value in zip(return_values, row):
                return_array.append(value)

        return [np.array(x) for x in return_values]

    def _get_values_for_columns(self, columns, line):
        results = []
        l_split = line.split()
        for id_this in columns:
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
        results[0] += self._id_offset
        return results

    def _read_column_names(self, f):
        return [x.split("(")[0] for x in f.readline().split()]





class AHFStatFile(HaloStatFile):
    _id_offset = 1

    _column_translations = {'n_dm': translations.Function(lambda ngas, nstar, npart: npart - ngas - nstar,
                                                          'n_gas', 'n_star', 'npart')}

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
        return file

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
    _id_offset = 0

    _column_translations = {'n_dm': translations.Rename('N_dark'),
                            'n_gas': translations.Rename('N_gas'),
                            'n_star': translations.Rename("N_star"),
                            'npart': translations.Function(lambda ngas, nstar, ndark: ngas + nstar + ndark,
                                                           "N_dark", "N_gas", "N_star")}

    @classmethod
    def filename(cls, timestep_filename):
        return timestep_filename + '.amiga.stat'

