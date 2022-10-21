import functools
import sys
from turtle import update

import pandas as pd
import tqdm

import isodata


def _get_args_dict(fn, args, kwargs):
    args_names = fn.__code__.co_varnames[: fn.__code__.co_argcount]
    return {**dict(zip(args_names, args)), **kwargs}


class support_date_range:
    def __init__(self, max_days_per_request, update_dates=None):
        """Maximum number of days that can be queried at once"""
        assert max_days_per_request > 0, "max_days_per_request must be greater than 0"
        self.max_days_per_request = max_days_per_request
        self.update_dates = update_dates

    def __call__(self, f):
        @functools.wraps(f)
        def wrapped_f(*args, **kwargs):
            args_dict = _get_args_dict(f, args, kwargs)

            if "date" in args_dict and "start" in args_dict:
                raise ValueError(
                    "Cannot supply both 'date' and 'start' to function {}".format(
                        f,
                    ),
                )

            if "date" not in args_dict and "start" not in args_dict:
                raise ValueError(
                    "Must supply either 'date' or 'start' to function {}".format(
                        f,
                    ),
                )

            if "start" in args_dict:
                args_dict["date"] = args_dict["start"]
                del args_dict["start"]

            args_dict["date"] = isodata.utils._handle_date(
                args_dict["date"],
                args_dict["self"].default_timezone,
            )

            # no date range handling required
            if "end" not in args_dict:
                return f(**args_dict)
            else:
                args_dict["end"] = isodata.utils._handle_date(
                    args_dict["end"],
                    args_dict["self"].default_timezone,
                )

            # use .date() to remove timezone info, which doesnt matter if just a date

            # if using python 3.7, there will be an older version of pandas installed that must used closed
            try:
                dates = pd.date_range(
                    args_dict["date"].date(),
                    args_dict["end"].date(),
                    freq=f"{self.max_days_per_request}D",
                    inclusive="left",
                )
            except TypeError:
                dates = pd.date_range(
                    args_dict["date"].date(),
                    args_dict["end"].date(),
                    freq=f"{self.max_days_per_request}D",
                    closed="left",
                )

            # add end date since it's not included
            dates = dates.tolist() + [args_dict["end"]]
            dates = [
                isodata.utils._handle_date(
                    d,
                    args_dict["self"].default_timezone,
                )
                for d in dates
            ]

            # sometime api have restrictions/optimizations based on date ranges
            # update_dates allows for the caller to insert this logic
            if self.update_dates is not None:
                dates = self.update_dates(dates, args_dict)

            start_date = dates[0]

            if self.max_days_per_request == 1:
                del args_dict["end"]

            all_df = []

            for end_date in tqdm.tqdm(
                dates[1:],
                # only do progress bar if more than one chunk
                disable=len(dates) <= 2,
            ):
                # if we come across None, it means we should reset
                if end_date is None:
                    start_date = None
                    continue

                # if start_date is None, we just reset and end is actually the start
                if start_date is None:
                    start_date = end_date
                    continue

                args_dict["date"] = start_date
                if self.max_days_per_request > 1:
                    args_dict["end"] = end_date

                df = f(**args_dict)

                all_df.append(df)
                start_date = end_date
            df = pd.concat(all_df).reset_index(drop=True)
            return df

        return wrapped_f


def pjm_update_dates(dates, args_dict):
    """PJM has a weird API. This method updates the date range list to account
    for the following restrictions:

     - date ranges cannot span year boundaries
     - date ranges cannot span archive / standard boundaries
     - date range is inclusive of start and end dates
    """
    new_dates = []

    for i, date in enumerate(dates):
        # stop if last date
        if i + 1 == len(dates):
            # add last date if new range has started
            if new_dates[-1] is not None:
                new_dates.append(date)

            break

        new_dates.append(date)

        # restriction 1: year boundary
        next_date = dates[i + 1]
        for year in range(date.year, next_date.year):
            current_year_end = pd.Timestamp(
                year=year,
                month=12,
                day=31,
                hour=23,
                minute=59,
                tz=args_dict["self"].default_timezone,
            )
            new_dates.append(current_year_end)
            next_year_start = pd.Timestamp(
                year=year + 1,
                month=1,
                day=1,
                hour=0,
                minute=0,
                tz=args_dict["self"].default_timezone,
            )

            new_dates.append(None)  # signal to skip to next date

            # dont need another range if the range ends at the start of the next year
            if next_year_start != next_date:
                new_dates.append(next_year_start)

    # remove trailing None
    if new_dates[-1] is None:
        new_dates = new_dates[:-1]

    return new_dates
