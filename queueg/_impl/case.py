





class Case:
    """
    Utility for parametrizing a script
    via "codes" that can be automatically generated by a manager script.

    Codes are strings. Example codes are: A1, A2, B1, B2.
    In this case, there are two factors `{A, B}` and `{1, 2}` which together
    generate four codes.
    The factors argument includes information used to assign meaning to codes.
    For this example, the factors might be:

    .. code-block:: python

        factors = {
            'beverage': {
                'A': ('coffee', prepare_coffee),
                'B': ('tea', prepare_tea),
            },
            'snack': {
                '1': ('cookies', prepare_cookies),
                '2': ('chips', prepare_chips),
            },
        }

    Parameters:
        factors (dict):
            a double-dictionary `{label: actions}`
            in which each object `actions` has the form `{var: (label, action)}`,
            where var is a single character in length, label is a string, and
            action is a function of the form `e |--> (action on e)`.
    """
    def __init__(self,
        factors,
        file=None,
    ):
        self.file = file
        self.factors = factors
        self.code = None
        for factor in self.factors:
            for var in self.factors[factor]:
                if len(var) != 1:
                    raise ValueError(f"factor variable {var} for factor {factor}: Each factor variable must be a single character in length. Please choose another variable.")


    def configure(self, e, code):
        """
        (Not called by user.)
        Configure the `e` instance according to the code.

        Usage: the application script calls `e`'s `set_case`
        method, and this method is called therein.

        Arguments:
            e (variable): target instance, modified by case actions
            code (string):

        :meta private:
        """
        # todo review: why preserve the code in self?
        self.code = code
        for idx, factor in enumerate(self.factors):
            var = code[idx]
            tup = self.factors[factor][var]
            action = tup[1]
            action(e)


    def list_all(self):
        """

        Returns:
            list of string: a list of all possible codes
        """
        lists = []
        # extract the parts we need for simplicity
        for factor in self.factors:
            d = self.factors[factor]
            tmp = []
            for label in d:
                tmp.append(label)
            lists.append(tmp)
        # e.g. labels = [[a,b],[q,r],[1,2]]
        # want aq1, aq2, ar1, ar2, ...
        # and it is nice to have it in that order.
        out = lists[len(lists)-1]
        for i in reversed(range(len(lists)-1)):
            tmp = []
            for x in lists[i]:
                tmp += [x+y for y in out]
            out = tmp
        return out


    def all(self):
        """

        Alias of :any:`Case.list_all`.

        """
        return self.list_all()


    def str_code(self, code):
        settings = []
        for idx, factor in enumerate(self.factors):
            var = code[idx]
            tup = self.factors[factor][var]
            action_label = tup[0]
            settings.append(f"{factor}: {action_label}")
        return ", ".join(settings)


    def __str__(self):
        setting_options_list = []
        setting_label_list = []
        for idx, factor in enumerate(self.factors):
            setting_options = []
            setting_dict = self.factors[factor]
            for var in setting_dict:
                tup = setting_dict[var]
                action_label = tup[0]
                setting_options.append(action_label)
            setting_label_list.append(factor)
            setting_options_list.append("{" + ", ".join(setting_options) + "}")
        zip_list = [": ".join(pair) for pair in zip(setting_label_list, setting_options_list)]
        return ", ".join(zip_list)


