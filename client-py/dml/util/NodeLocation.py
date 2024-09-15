class NodeLocation:
    """
    Node Location
    """

    def __init__(self, region: str, access_point: str, provider: str):
        self.region = region
        self.access_point = access_point
        self.provider = provider

    def get_region(self) -> str:
        return self.region

    def get_access_point(self) -> str:
        return self.access_point

    def get_provider(self) -> str:
        return self.provider
