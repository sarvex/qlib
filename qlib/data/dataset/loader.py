# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
import abc
import warnings
import numpy as np
import pandas as pd

from typing import Tuple, Union, List, Type

from qlib.data import D
from qlib.data import filter as filter_module
from qlib.data.filter import BaseDFilter
from qlib.utils import load_dataset, init_instance_by_config, time_to_slc_point, get_callable_kwargs
from qlib.log import get_module_logger


class DataLoader(abc.ABC):
    """
    DataLoader is designed for loading raw data from original data source.
    """

    @abc.abstractmethod
    def load(self, instruments, start_time=None, end_time=None) -> pd.DataFrame:
        """
        load the data as pd.DataFrame.

        Example of the data (The multi-index of the columns is optional.):

            .. code-block:: python

                                        feature                                                             label
                                        $close     $volume     Ref($close, 1)  Mean($close, 3)  $high-$low  LABEL0
                datetime    instrument
                2010-01-04  SH600000    81.807068  17145150.0       83.737389        83.016739    2.741058  0.0032
                            SH600004    13.313329  11800983.0       13.313329        13.317701    0.183632  0.0042
                            SH600005    37.796539  12231662.0       38.258602        37.919757    0.970325  0.0289


        Parameters
        ----------
        instruments : str or dict
            it can either be the market name or the config file of instruments generated by InstrumentProvider.
        start_time : str
            start of the time range.
        end_time : str
            end of the time range.

        Returns
        -------
        pd.DataFrame:
            data load from the under layer source
        """
        pass


class DLWParser(DataLoader):
    """
    (D)ata(L)oader (W)ith (P)arser for features and names

    Extracting this class so that QlibDataLoader and other dataloaders(such as QdbDataLoader) can share the fields.
    """

    def __init__(self, config: Union[list, tuple, dict]):
        """
        Parameters
        ----------
        config : Union[list, tuple, dict]
            Config will be used to describe the fields and column names

            .. code-block::

                <config> := {
                    "group_name1": <fields_info1>
                    "group_name2": <fields_info2>
                }
                or
                <config> := <fields_info>

                <fields_info> := ["expr", ...] | (["expr", ...], ["col_name", ...])
                # NOTE: list or tuple will be treated as the things when parsing
        """
        self.is_group = isinstance(config, dict)

        if self.is_group:
            self.fields = {grp: self._parse_fields_info(fields_info) for grp, fields_info in config.items()}
        else:
            self.fields = self._parse_fields_info(config)

    def _parse_fields_info(self, fields_info: Union[list, tuple]) -> Tuple[list, list]:
        if len(fields_info) == 0:
            raise ValueError("The size of fields must be greater than 0")

        if not isinstance(fields_info, (list, tuple)):
            raise TypeError("Unsupported type")

        if isinstance(fields_info[0], str):
            exprs = names = fields_info
        elif isinstance(fields_info[0], (list, tuple)):
            exprs, names = fields_info
        else:
            raise NotImplementedError("This type of input is not supported")
        return exprs, names

    @abc.abstractmethod
    def load_group_df(
        self,
        instruments,
        exprs: list,
        names: list,
        start_time: Union[str, pd.Timestamp] = None,
        end_time: Union[str, pd.Timestamp] = None,
        gp_name: str = None,
    ) -> pd.DataFrame:
        """
        load the dataframe for specific group

        Parameters
        ----------
        instruments :
            the instruments.
        exprs : list
            the expressions to describe the content of the data.
        names : list
            the name of the data.

        Returns
        -------
        pd.DataFrame:
            the queried dataframe.
        """
        pass

    def load(self, instruments=None, start_time=None, end_time=None) -> pd.DataFrame:
        if self.is_group:
            return pd.concat(
                {
                    grp: self.load_group_df(
                        instruments, exprs, names, start_time, end_time, grp
                    )
                    for grp, (exprs, names) in self.fields.items()
                },
                axis=1,
            )
        exprs, names = self.fields
        return self.load_group_df(instruments, exprs, names, start_time, end_time)


class QlibDataLoader(DLWParser):
    """Same as QlibDataLoader. The fields can be define by config"""

    def __init__(
        self,
        config: Tuple[list, tuple, dict],
        filter_pipe: List = None,
        swap_level: bool = True,
        freq: Union[str, dict] = "day",
        inst_processor: dict = None,
    ):
        """
        Parameters
        ----------
        config : Tuple[list, tuple, dict]
            Please refer to the doc of DLWParser
        filter_pipe :
            Filter pipe for the instruments
        swap_level :
            Whether to swap level of MultiIndex
        freq:  dict or str
            If type(config) == dict and type(freq) == str, load config data using freq.
            If type(config) == dict and type(freq) == dict, load config[<group_name>] data using freq[<group_name>]
        inst_processor: dict
            If inst_processor is not None and type(config) == dict; load config[<group_name>] data using inst_processor[<group_name>]
        """
        if filter_pipe is not None:
            assert isinstance(filter_pipe, list), "The type of `filter_pipe` must be list."
            filter_pipe = [
                init_instance_by_config(fp, None if "module_path" in fp else filter_module, accept_types=BaseDFilter)
                for fp in filter_pipe
            ]

        self.filter_pipe = filter_pipe
        self.swap_level = swap_level
        self.freq = freq

        # sample
        self.inst_processor = inst_processor if inst_processor is not None else {}
        assert isinstance(self.inst_processor, dict), f"inst_processor(={self.inst_processor}) must be dict"

        super().__init__(config)

        if self.is_group and isinstance(freq, dict):
            for _gp in config.keys():
                if _gp not in freq:
                    raise ValueError(f"freq(={freq}) missing group(={_gp})")
            assert (
                self.inst_processor
            ), f"freq(={self.freq}), inst_processor(={self.inst_processor}) cannot be None/empty"

    def load_group_df(
        self,
        instruments,
        exprs: list,
        names: list,
        start_time: Union[str, pd.Timestamp] = None,
        end_time: Union[str, pd.Timestamp] = None,
        gp_name: str = None,
    ) -> pd.DataFrame:
        if instruments is None:
            warnings.warn("`instruments` is not set, will load all stocks")
            instruments = "all"
        if isinstance(instruments, str):
            instruments = D.instruments(instruments, filter_pipe=self.filter_pipe)
        elif self.filter_pipe is not None:
            warnings.warn("`filter_pipe` is not None, but it will not be used with `instruments` as list")

        freq = self.freq[gp_name] if isinstance(self.freq, dict) else self.freq
        df = D.features(
            instruments, exprs, start_time, end_time, freq=freq, inst_processors=self.inst_processor.get(gp_name, [])
        )
        df.columns = names
        if self.swap_level:
            df = df.swaplevel().sort_index()  # NOTE: if swaplevel, return <datetime, instrument>
        return df


class StaticDataLoader(DataLoader):
    """
    DataLoader that supports loading data from file or as provided.
    """

    def __init__(self, config: dict, join="outer"):
        """
        Parameters
        ----------
        config : dict
            {fields_group: <path or object>}
        join : str
            How to align different dataframes
        """
        self.config = config
        self.join = join
        self._data = None

    def __getstate__(self) -> dict:
        # avoid pickling `self._data`
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}

    def load(self, instruments=None, start_time=None, end_time=None) -> pd.DataFrame:
        self._maybe_load_raw_data()
        if instruments is None:
            df = self._data
        else:
            df = self._data.loc(axis=0)[:, instruments]
        if start_time is None and end_time is None:
            return df  # NOTE: avoid copy by loc
        # pd.Timestamp(None) == NaT, use NaT as index can not fetch correct thing, so do not change None.
        start_time = time_to_slc_point(start_time)
        end_time = time_to_slc_point(end_time)
        return df.loc[start_time:end_time]

    def _maybe_load_raw_data(self):
        if self._data is not None:
            return
        self._data = pd.concat(
            {fields_group: load_dataset(path_or_obj) for fields_group, path_or_obj in self.config.items()},
            axis=1,
            join=self.join,
        )
        self._data.sort_index(inplace=True)


class DataLoaderDH(DataLoader):
    """DataLoaderDH
    DataLoader based on (D)ata (H)andler
    It is designed to load multiple data from data handler
    - If you just want to load data from single datahandler, you can write them in single data handler

    TODO: What make this module not that easy to use.
    - For online scenario
        - The underlayer data handler should be configured. But data loader doesn't provide such interface & hook.
    """

    def __init__(self, handler_config: dict, fetch_kwargs: dict = {}, is_group=False):
        """
        Parameters
        ----------
        handler_config : dict
            handler_config will be used to describe the handlers

            .. code-block::

                <handler_config> := {
                    "group_name1": <handler>
                    "group_name2": <handler>
                }
                or
                <handler_config> := <handler>
                <handler> := DataHandler Instance | DataHandler Config

        fetch_kwargs : dict
            fetch_kwargs will be used to describe the different arguments of fetch method, such as col_set, squeeze, data_key, etc.

        is_group: bool
            is_group will be used to describe whether the key of handler_config is group

        """
        from qlib.data.dataset.handler import DataHandler

        if is_group:
            self.handlers = {
                grp: init_instance_by_config(config, accept_types=DataHandler) for grp, config in handler_config.items()
            }
        else:
            self.handlers = init_instance_by_config(handler_config, accept_types=DataHandler)

        self.is_group = is_group
        self.fetch_kwargs = {"col_set": DataHandler.CS_RAW} | fetch_kwargs

    def load(self, instruments=None, start_time=None, end_time=None) -> pd.DataFrame:
        if instruments is not None:
            get_module_logger(self.__class__.__name__).warning(f"instruments[{instruments}] is ignored")

        return (
            pd.concat(
                {
                    grp: dh.fetch(
                        selector=slice(start_time, end_time),
                        level="datetime",
                        **self.fetch_kwargs
                    )
                    for grp, dh in self.handlers.items()
                },
                axis=1,
            )
            if self.is_group
            else self.handlers.fetch(
                selector=slice(start_time, end_time),
                level="datetime",
                **self.fetch_kwargs
            )
        )
