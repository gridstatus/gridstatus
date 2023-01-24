DATAVIEWER_LMP_URL = "https://dataviewer.pjm.com/dataviewer/pages/public/lmp.jsf"

LMP_PARTIAL_RENDER_ID = "formLeftPanel:topLeftGrid"

DV_LMP_RECENT_NUM_DAYS = 3


class PJMDataViewer:
    def __init__(self, pjm):
        self.pjm = pjm
