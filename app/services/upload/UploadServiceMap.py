"""
Upload Service Map
Maps DataSource enum to service implementations
"""
from app.core.enum.DataSource import DataSource
from app.services.upload.ZomatoDataService import ZomatoDataService
from app.services.upload.PosOrderService import PosOrderService

# Initialize services
zomatoDataService = ZomatoDataService()
posOrderService = PosOrderService()


def createServiceMap():
    """Create service map matching devyani_python structure"""
    serviceMap = {
        DataSource.POS_ORDERS: posOrderService,
        DataSource.ZOMATO: zomatoDataService,
        # Add more services as needed
        # DataSource.SWIGGY: swiggyDataService,
        # DataSource.TRM_MPR: trmMprService,
    }
    return serviceMap


serviceMap = createServiceMap()

