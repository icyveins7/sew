import numpy as np
import sys
import configparser


#%%
class BlobInterpreter:
    """
    Class to interpret a blob.

    Interpreting a blob involves a known structure, which specifies
    1) Descriptor (short string describing this field)
    2) Type (word length i.e number of bits, and the type to cast to)

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
        """
        Creates a BlobInterpreter based on a specified structure.

        Parameters
        ----------
        structure : list
            Ordered list of tuples, where each tuple is of the form (descriptor, typestr).
            Type strings are specified in STR_TO_TYPE.
            Descriptors are fieldnames, usually used to identify the purpose of that section of data.
        """
        if not isinstance(structure, list):
            raise TypeError('Structure must be a list of tuples')
        self._structure = structure

    @classmethod
    def fromDictionary(cls, structure: dict):
        """
        Generates a BlobInterpreter from a dictionary.
        Dictionaries are expected to be ordered as of Python 3.7.
        """
        version = sys.version_info
        if version.major < 3 or (version.major == 3 and version.minor < 7):
            raise TypeError("Dictionary interpretation requires Python 3.7 or higher")
        return cls([(k, v) for k, v in structure.items()])
    
    @classmethod
    def fromConfig(cls, configfilepath: str, sectionname: str):
        """
        Generates a BlobInterpreter from a configuration file.
        This is loaded with 'configparser'; see https://docs.python.org/3/library/configparser.html.
        Config files are expected to be ordered as of Python 3.7.
        """
        version = sys.version_info
        if version.major < 3 or (version.major == 3 and version.minor < 7):
            raise TypeError("Configparser interpretation requires Python 3.7 or higher")

        cfg = configparser.ConfigParser()
        cfg.read(configfilepath)
        section = cfg[sectionname]
        return cls([(k, v) for k, v in section.items()])

    def appendField(self,  descriptor: str, type: str='u8'):
        """
        Appends a field to the structure.

        Parameters
        ----------
        descriptor : str
            Description of the field.
        type : str
            String that specifies the type to cast to.
            Defaults to 'u8'.
        """
        self._structure.append((descriptor, type))

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
        for desc, typestr in self._structure:
            # Turn it into a numpy array
            interpreted = np.frombuffer(
                blob[ptr:ptr+self.STR_TO_SIZE[typestr]],
                dtype=self.STR_TO_TYPE[typestr]
            )

            ptr += self.STR_TO_SIZE[typestr]

            output[desc] = interpreted

        return output



        

    