import numpy as np


#%%
class BlobInterpreter:
    """
    Class to interpret a blob.

    Interpreting a blob involves a known structure, which specifies
    1) Type (word length i.e number of bits, and the type to cast to)
    2) Descriptor (short string describing this field)

    Internally, this is kept as a list of tuples.

    This can be used to automatically generate multiple columns when selecting from a blob.
    """

    # Define type dictionary
    STR_TO_TYPE = {
        'u8': np.uint8,
        'u16': np.uint16,
        'u32': np.uint32,
        'u64': np.uint64,
        'i8': np.int8,
        'i16': np.int16,
        'i32': np.int32,
        'i64': np.int64,
        'f32': np.float32,
        'f64': np.float64,
        'fc32': np.complex64,
        'fc64': np.complex128
    }

    STR_TO_SIZE = {
        'u8': 1,
        'u16': 2,
        'u32': 4,
        'u64': 8,
        'i8': 1,
        'i16': 2,
        'i32': 4,
        'i64': 8,
        'f32': 4,
        'f64': 8,
        'fc32': 8,
        'fc64': 16
    }

    def __init__(self, structure: list=[]):
        self._structure = structure

    def _generateUnknownDescriptor(self, index: int):
        return "unknown%d" % index

    def appendField(self, type: str='u8', descriptor: str=None):
        """
        Appends a field to the structure.

        Parameters
        ----------
        type : str
            String that specifies the type to cast to.
            Defaults to 'u8'.
        descriptor : str, optional
            Description of the field. Defaults to None,
            which will use an 'unknown' descriptor with an index.
        """
        self._structure.append(
            (type, 
             descriptor if descriptor is not None else self._generateUnknownDescriptor(len(self._structure)))
        )

    def interpret(self, blob: bytes) -> dict:
        """
        Interprets a blob and returns a dict of arrays according to the structure.

        Parameters
        ----------
        blob : bytes
            Bytes object, usually obtained from a BLOB column select.

        Returns
        -------
        output : dict
            Dictionary of arrays, according to the internal structure.
        """

        output = dict()
        ptr = 0
        for typestr, desc in self._structure:
            interpreted = np.frombuffer(
                blob[ptr:ptr+self.STR_TO_SIZE[typestr]],
                dtype=self.STR_TO_TYPE[typestr]
            )

            ptr += self.STR_TO_SIZE[typestr]

            output[desc] = interpreted

        return output



        

    