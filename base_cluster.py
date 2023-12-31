from abc import ABC, abstractmethod

class BaseCluster(ABC):

	@abstractmethod
	async def run():
		...