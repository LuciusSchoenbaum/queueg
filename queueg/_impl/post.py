


from os.path import \
    join as os_path_join, \
    exists as os_path_exists
from numpy import \
    linspace as numpy_linspace, \
    meshgrid as numpy_meshgrid, \
    hstack as numpy_hstack


from mv1fw import (
    create_dir,
    get_fslabels,
    parse_labels,
    CogManager,
    Logger,
    Reference,
)
from mv1fw.visutil import (
    Figure,
    Animation,
)

from .figureofmerit import FigureofMerit



class Post:
    """
    Instances of utilities for data processing, plotting, ...
    for :any:`Reference` instances that have
    (at a time prior to calling this class's init method)
    generated and stored their data.

    The :any:`Post` instance can be used to
    generate standard output for quick/convenient review,
    or its fields can be used to write custom output routines.

    Arguments:

        run (run instance, for example :any:`PyPinnchRun`):
            running instance to use to extract the root path.
            If None, the path must be provided by other means.
    """

    def __init__(
            self,
            run = None,
    ):
        self.fig = Figure()
        self.anim = Animation()
        self.path = None
        self.cog = None
        self.cog_id = 0
        self.reference = None
        # todo do something with the post log
        self.log = Logger()
        if run is not None:
            self.attach_path(explicit_path=run.path)


    def attach_path(
            self,
            location = None,
            explicit_path = None,
    ):
        """
        Associate a path, which can be a locator for multiple references.
        Can be abstractly defined as a location, or
        as an explicit path.

        Arguments:

            location (:any:`Location`):
            explicit_path (optional string):

        """
        self.path = location.get_path(
            create=False,
            explicit_conf=None,
        ) if explicit_path is None else explicit_path
        # > possibly re-__init__
        self.cog = CogManager(
            output_abs_path_root=self.path,
        )



    def attach_reference(
        self,
        labels,
        stem = None,
        tolerance = 1e-4,
        data_format = 'mv1',
    ):
        """
        Attach a reference, after calling :any:`attach_path`.

        Once a reference is attached, it can be used to
        perform post-processing (such as plotting).
        When this task is completed, another reference (dataset)
        can be attached, so
        if there are multiple references (datasets) of interest,
        they can be treated serially.


        For each reference, custom routines can be written,
        utilizing the self.fig and self.anim and result.cog instances.
        Alternatively, plot_reference() method can be called
        for quick and easy "good enough" output creation.

        Arguments:

            labels (string):
            stem (optional string):
                An optional jump to a directory, mainly used in case of
                PyPinnch directory trees, with action subdirectories.
            tolerance (scalar):
                tolerance for time slice selection. Default: `1e-4`
                Advice: when it really counts, set up the work so that
                you select timeslices exactly as they appear in the reference data.
                But general cases are allowed so that you can quickly scale down
                for a test or dryrun.
            data_format (string):
                The format of the data.
                Default:
                ``mv1`` basic multi-variable output format.

        Returns:

            boolean, whether path is successfully attached or not.

    """
        # todo Two references can be managed at the same time by ()()() todo

        if self.path is None:
            raise ValueError("Reference was attached, but path was not set. Did you first call attach_path?")
        # todo other data formats.
        if data_format != 'mv1':
            raise NotImplementedError
        path = os_path_join(self.path, stem) if stem is not None else self.path
        self.reference = Reference(
            path=path,
        )
        return self.reference.init(
            labels=labels,
            log=self.log,
            tolerance=tolerance,
        )


    def plot_reference(
            self,
            style = 'default',
    ):
        """
        A generic plotting routine that generates figures and animations.

        If a reference is "attached" then the self.ref field is populated
        with a :any:`Reference` instance that "points" to a dataset
        stored on disk in ``mv1`` format or ``ts1`` format.

        Example of files stored in mv1 format::

            x-y--u.toml
            x-y--u_000.dat
            x-y--u_100.dat
            x-y--u_200.dat

        Example of time series, stored in mv1 format (special case)::

            t--u.toml
            t--u.dat

        The variable 't' can appear as input only in case of time series.

        Arguments:

            style (string):
                Default: 'default', options::

                    - 'default' : plot as multiseries for 1d-1d inputs,
                        or as a heatmap for 2d-1d inputs.
                    - 'scatterplot' : set if you wish to
                        plot 1, 2, or 3 outputs in a scatterplot figure.
                        A movie is created to show progress as time advances.

        """

        ref = self.reference

        ########
        # ref typecheck
        if ref is None:
            raise ValueError(f"No data attached. First call attach_output().")
        # todo if ref uninitialized, fail gracefully
        #######

        lbl = ref.lbl
        indim = ref.indim
        with_t = ref.with_t
        ranges = ref.get_ranges()

        if style == 'scatterplot':
            outdim = len(lbl[indim:])
            fslabels = ref.fslabels
            if indim == 0:
                raise NotImplementedError
            elif indim == 1:
                raise NotImplementedError
            elif indim == 2:
                for ti, t in ref.get_times():
                    XU = ref.get_XU(t = t)
                    filename = self.cog.filename(
                        action=self,
                        handle=fslabels + f".t{ti}",
                        ending="png",
                    )
                    if outdim == 2:
                        # common case of an easily surveyable scatterplot
                        self.fig.scatterplot(
                            filename = filename,
                            title = ', '.join(lbl[indim:]) + f", t = {t:.3f}",
                            X = XU[:,indim:],
                            # Xslabels = None, # lbl[indim:]
                            xlim = ranges[lbl[0]],
                            ylim = ranges[lbl[1]],
                        )
                    else:
                        # case that is more ambiguous
                        # > add code as the need arises
                        raise NotImplementedError
                frame_glob = self.cog.filename(
                    action=self,
                    handle=f"{fslabels}.t[0-9]+",
                    ending="png",
                )
                filename = self.cog.filename(
                    action=self,
                    handle=f"{fslabels}",
                    ending="gif",
                )
                ok = self.anim.from_frame_glob(
                    frame_glob=frame_glob,
                    filename=filename,
                    # duration=100,
                    sort_nicely=True,
                )
                if not ok:
                    print(f"Could not make movie from glob {frame_glob}")
        else:
            if indim == 0:
                # In this case with_t == True.
                # > time series
                # Assumption: time series do not need to be sorted.
                XYs = ref.get_TUs()
                # todo review - this doesn't look right, but it seems to work.
                tmin = XYs[0][0,0]
                tmax = XYs[0][-1,0]
                filename = self.cog.filename(
                    action=self,
                    handle=ref.fslabels,
                    ending="png",
                )
                self.fig.multiseries(
                    filename=filename,
                    title="",
                    text="",
                    XYs=XYs,
                    inlabel='t',
                    outlabels=lbl,
                    xlim=(tmin, tmax),
                    ylim=ranges[lbl[0]] if len(lbl) == 1 else None,
                )
            else:
                for outidx in range(indim, len(lbl)):
                    fslabels = get_fslabels(lbl[:indim]+[lbl[outidx]], indim, with_t)
                    for ti, t in ref.get_times():
                        XU = ref.get_XU(t = t)
                        filename = self.cog.filename(
                            action=self,
                            handle=fslabels + f".t{ti}",
                            ending="png",
                        )
                        if indim == 0:
                            raise NotImplementedError
                        elif indim == 1:
                            self.fig.series(
                                filename=filename,
                                title="",
                                text="",
                                X=XU,
                                inlabel=lbl[0],
                                inidx=0,
                                outlabels=lbl[outidx],
                                outidxs=[outidx],
                                t=t,
                                xlim=ranges[lbl[0]],
                                ylim=ranges[lbl[outidx]],
                                reorder=True,
                            )
                        elif indim == 2:
                            self.fig.heatmap(
                                filename=filename,
                                X=XU,
                                in1=0,
                                in2=1,
                                out1=outidx,
                                lbl=lbl,
                                title="",
                                t=t,
                                value_range=ranges[lbl[outidx]],
                                xlim=ranges[lbl[0]],
                                ylim=ranges[lbl[1]],
                                plot_xray=True if ti == 0 else False,
                            )
                        else: # indim >= 3
                            raise NotImplementedError
                    frame_glob = self.cog.filename(
                        action=self,
                        handle=f"{fslabels}.t[0-9]+",
                        ending="png",
                    )
                    filename = self.cog.filename(
                        action=self,
                        handle=f"{fslabels}",
                        ending="gif",
                    )
                    ok = self.anim.from_frame_glob(
                        frame_glob=frame_glob,
                        filename=filename,
                        # duration=100,
                        sort_nicely=True,
                    )
                    if not ok:
                        print(f"Could not make movie from glob {frame_glob}")



    def attach_reference2(
            self,
            path,
    ):
        """
        Attach a second reference,
        possibly to compare them.

        Arguments:

            path (string):

        """
        # > check if our attached labels are
        #  in the suggested path

        # YES:
        # populate a field with a reference

        # NO:
        # value error

        # todo


    def validate(self):
        """
        todo

        """
        # > check if there is output

        # > check if there is a reference

        # YES: validate,

        # todo




    def mesh(
            self,
            ranges,
            resolution,
            right_open,
    ):
        """
        Construct a regular array of points
        using a standard meshgrid method,
        formatted as an :any:`XFormat` input.

         Arguments:

            ranges (nonempty list of pair of scalar):
                length is ``d``, the dimension of the mesh,
                defines the ranges as well as the order
                of the coordinates.
            resolution (integer or list of integers):
                resolution ``n``, the number of points
                in the regular mesh along each dimension.
            right_open (boolean):
                whether to create a right-open regular mesh.

        Returns:

            numpy.tensor
                an array of shape ``(n**d, d)``.

        """
        # > set up resolution list
        if isinstance(resolution, list):
            resns = resolution
        else:
            resns = len(ranges)*[resolution]
        # > build inputs to meshgrid
        sps = ()
        for rn, resn in zip(ranges, resns):
            if right_open:
                # It is surprising that this is so much trouble, but
                # the torch.linspace API is fairly rigid, ITCINOOD.
                delta = (rn[1] - rn[0])/resn
                end = rn[1] - delta
            else:
                end = rn[1]
            # numpy's linspace has an argument `endpoint`
            # but torch's linspace doesn't have it, so we'll do it like this.
            sp = numpy_linspace(start=rn[0], stop=end, num=resn)
            sps += (sp,)
        Xout = numpy_meshgrid(*sps, indexing='ij')
        # > reshape/reformat Xout, I am not sure what is the most efficient way
        Xlist = ()
        for v in Xout:
            Xlist += (v.reshape((-1, 1)),)
        out = numpy_hstack(Xlist)
        return out





    def validation(
            self,
            label=None,
            labels=None,
            good_thresh = 0.01,
            maybe_thresh = 0.07,
    ):
        """
        Perform a simple routine summarizing
        the validation of a run.
        It can be made more general-use,
        but this is best done on an as-needed basis.

        Arguments:

            label (optional string):
                target output label.
            labels (string):
                target output labels, comma-separated, replaces ``label``.
            good_thresh (optional scalar):
                A validation measurement below which
                the validation is deemed satisfactory.
                Default: 0.01
            maybe_thresh (optional scalar):
                A validation measurement below which
                the validation is deemed questionable.
                Default: 0.07

        Returns:

            summary (:any:`FigureofMerit`):
                The worst case validation error,
                and judgment string, one of:
                'Good', 'Probably Bad', and 'Bad'.

        """
        labels_ = labels if labels is not None else label
        lbl, _, _ = parse_labels(labels_)
        lblval = [f"{lb}val" for lb in lbl]
        out = ()
        # > try both (1) and (2) under the hood,
        # it is trade-off for the user's convenience.
        # > try (1) time-dependent data
        if self.attach_reference(f"t;{','.join(lblval)}", stem='Result'):
            # > get validation error
            U = self.reference.get_XU()
            # > assess data: list of t, f1val, f2val, ...
            for i, lb in enumerate(lblval):
                # > skip the zeroth timestep,
                # it is sometimes exceptional
                worstcase_validation_error = float(U[1:,1+i].max())
                # > judgment string
                if worstcase_validation_error < good_thresh:
                    judgment = "Good"
                elif worstcase_validation_error < maybe_thresh:
                    judgment = "Probably Bad"
                else:
                    judgment = "Bad"
                out += FigureofMerit(
                    value=worstcase_validation_error,
                    judgment=judgment,
                    label=lb,
                ),
        else:
            # > try (2) time-independent data
            if self.attach_reference(f";{','.join(lblval)}", stem='Result'):
                # > get validation error
                U = self.reference.get_XU()
                for i, lb in enumerate(lbl):
                    # > assess data: a single scalar
                    worstcase_validation_error = U[0,i]
                    # > judgment string
                    if worstcase_validation_error < good_thresh:
                        judgment = "Good"
                    elif worstcase_validation_error < maybe_thresh:
                        judgment = "Probably Bad"
                    else:
                        judgment = "Bad"
                    out += FigureofMerit(
                        value=worstcase_validation_error,
                        judgment=judgment,
                        label=lb,
                    ),
            else:
                s = "s" if len(lblval) > 1 else ""
                print(f"[Warning] [Post::validation] Cannot find validation data for label{s} {labels_}.")
                for lb in lbl:
                    out += FigureofMerit(
                        value=1000.0,
                        judgment="Bad",
                        label=lb,
                    ),
        if len(out) == 1:
            out = out[0]
        return out


    def validation_summary(
            self,
            label = None,
            labels = None,
            good_thresh = 0.01,
            maybe_thresh = 0.07,
    ):
        """
        As a convenience, generate a
        readable string for the information
        given by the validation() method.

        Arguments:

            label (optional string):
                target output label.
            labels (string):
                target output labels, comma-separated, replaces ``label``.
            good_thresh:
            maybe_thresh:

        Returns:

            string

        """
        # todo summary methods accept a location argument to store directly?

        # todo if there are multiple labels grouped in output,
        #  this method will fail if the user requests a subset of those labels.
        #  In other words, ITCINOOD, this method forces
        #  the structure of the output to propagate to the post-processing
        #  point of entry. The Right Thing (so to say)
        #  is to give the user the 'right' or 'freedom' to request specifically
        #  what they want to see. The problem with that is that it more
        #  labor than I wish to put in immediately (i.e. today), but it is not
        #  so much work as to be prohibitive using Python.
        #  it's a few extra branches and some file system scanning.

        vals = self.validation(
            labels=label if label is not None else labels,
            good_thresh=good_thresh,
            maybe_thresh=maybe_thresh,
        )
        out = "\n"
        if isinstance(vals, tuple):
            for val in vals:
                out += f"< {val.label} validation error \n> {val.value:.2}\n< Good or Bad? \n> {val.judgment}\n"
        else:
            out += f"< {vals.label} validation error \n> {vals.value:.2}\n< Good or Bad? \n> {vals.judgment}\n"
        return out




    def timing(self):
        """
        Get the timing of sections of the run.
        Works only for PyPinnch ITCINOOD.

        Returns:

            dict of string to float

        """
        # todo toml loader
        out = {}
        # todo awk
        path = os_path_join(self.path, 'Info', 'dat', 'timing.dat')
        if os_path_exists(path):
            with open(path) as f:
                safety = 0
                while f:
                    l = f.readline()
                    if l:
                        x, t = l.split(',')
                        out[x] = float(t)
                    else:
                        break
                    safety += 1
                    if safety == 200:
                        break
        else:
            print(f"[Warning] [Post::timing] timing data was not found.")
            # todo awk
            out['ttx'] = 1000.0
        return out


    def timing_summary(self):
        """
        Create a summary of the timing
        data obtained by timing().

        Returns:

            string

        """
        # todo summary methods accept a location argument
        #  to store directly

        # todo improve:
        # > xxxxxxxxxxxxxx ttx 53.4
        # > xxxxxxxx f0_moment 34.4
        # > xxx communication 12.34
        # etc.

        out = ""
        timing = self.timing()
        for x in timing:
            out += f"{x} {timing[x]}"
        return out



