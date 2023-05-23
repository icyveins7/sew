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

    Dynamically sized sections are also supported.
    To specify a dynamically sized section, suffix the descriptor with "_dyn".

    There are 3 scenarios of dynamically sized sections:
    ------------------------------------------------------
    1) "Flex" arrays: only 1 of these are allowed per structure.
    These will attempt to occupy as much of the blob as possible.
    For example, a structure with
        a: u8
        b_dyn: u8
        c: u16

    will interpret a 10-byte blob as having a 7-byte array assigned to "b_dyn".
    ------------------------------------------------------
    2) "Externally fixed length" arrays: the length is specified in the descriptor as well,
    like "_3_dyn".

    For example, a structure with
        a: u8
        b_3_dyn: u8
        c: u16

    requires a 6-byte blob, where "b_3_dyn" is a 3-byte array
    from the 2nd byte to the 4th byte i.e. [1:4]
    ------------------------------------------------------
    3) "Internally fixed length" arrays: the length is specified in another field.
    These are denoted with "_?_dyn", and the associated field must have the same name,
    but suffixed with "_len".

    For example, a structure with
        a_len: u8
        a_?_dyn: u8

    will read "a_len" for the number of elements, then use that as the size of
    "a_?_dyn". This is commonly found in headers of packets.
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

    STR_TO_CSTR = {
        'u8': '%hhu',
        'u16': '%hu',
        'u32': '%u',
        'u64': '%lu',
        'i8': '%hhd',
        'i16': '%hd',
        'i32': '%d',
        'i64': '%ld',
        'f32': '%f',
        'f64': '%f',
        # 'fc32': '%f', # These are a little complicated, let's not handle them for now
        # 'fc64': '%f
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
    
    def generateSplitStatement(self, blobColumnName: str, hexOutput: bool=False):
        """
        Generates SQL statement fragments that correspond to 
        splitting a blob into multiple columns based on the structure.

        Parameters
        ----------
        blobColumnName : str
            The SQLite column name that contains the BLOB.
        hexOutput : bool
            Flag to determine whether to return as hex strings, useful for views.
            The default is False, which will return as raw BLOBs.
        """
        output = []
        ptr = 1 # Sqlite substr starts from 1
        for desc, typestr in self._structure:
            size = self.STR_TO_SIZE[typestr]
            fragment = f'substr({blobColumnName},{ptr},{size})'
            if hexOutput:
                fragment = f'hex({fragment})'
            fragment = f'{fragment} AS {desc}'
            output.append(fragment)
            ptr += size

        return output
    
    def hexifyBlob(self, blob: bytes) -> str:
        """
        Returns a hex string form of the blob, akin to what SQLite
        would produce with its hex() function.

        Parameters
        ----------
        blob : bytes
            Input bytes object. This may come from a slice of the
            SQLite BLOB column.

        Returns
        -------
        h : str
            Hex string formatted with %02X.
        """
        h = "".join(["%02X" % i for i in blob])

        return h



        

    