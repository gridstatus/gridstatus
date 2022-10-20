import functools
import sys

import pandas as pd
import tqdm

import isodata


def _get_args_dict(fn, args, kwargs):
    args_names = fn.__code__.co_varnames[: fn.__code__.co_argcount]
    return {**dict(zip(args_names, args)), **kwargs}


class support_date_range:
    def __init__(self, max_days_per_request):
        """Maximum number of days that can be queried at once"""
        assert max_days_per_request > 0, "max_days_per_request must be greater than 0"
        self.max_days_per_request = max_days_per_request

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
            start_date = dates[0]

            if self.max_days_per_request == 1:
                del args_dict["end"]

            all_df = []
            for end_date in tqdm.tqdm(
                dates[1:],
                disable=False,
            ):
                args_dict["date"] = start_date
                if self.max_days_per_request > 1:
                    args_dict["end"] = end_date

                df = f(**args_dict)

                all_df.append(df)
                start_date = end_date
            df = pd.concat(all_df)
            return df

        return wrapped_f
