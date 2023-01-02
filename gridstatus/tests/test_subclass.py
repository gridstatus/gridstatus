import gridstatus


class ISOTest:
    iso = None

    def test_init(self):
        assert self.iso()


class TestPJM(ISOTest):
    iso = gridstatus.PJM


class TestMISO(ISOTest):
    iso = gridstatus.MISO

    def test_miso_specific_thing(self):
        assert self.iso.iso_id == "miso"
