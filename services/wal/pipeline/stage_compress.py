
# TODO  this class is not used and is not ready for operation yet
class CompressStage:
    def __init__(self, compression_algorithm):
        self.compression_algorithm = compression_algorithm

    def compress(self, data):
        if self.compression_algorithm == 'gzip':
            import gzip
            return gzip.compress(data)
        elif self.compression_algorithm == 'bz2':
            import bz2
            return bz2.compress(data)
        elif self.compression_algorithm == 'lzma':
            import lzma
            return lzma.compress(data)
        else:
            raise ValueError(f"Unsupported compression algorithm: {self.compression_algorithm}")
        
    