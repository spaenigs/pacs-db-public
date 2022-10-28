import struct
import warnings

warnings.filterwarnings("ignore", category=UserWarning)

from pydicom import dcmread
from sklearn.feature_extraction.text import CountVectorizer
from pathlib import Path
from pydicom.multival import MultiValue

import pandas as pd
import numpy as np

from joblib import dump


class TextEncoder:

    def __init__(self, df, modality, tag, directory):
        """
        A class to encode DICOM text tags as numerical vectors. Supported tags are SeriesDescription and
        ProtocolName.
        """
        self.df = df
        self.modality = modality
        self.tag = tag
        self.vectorizer = None
        self.X = None
        self.y = None
        self.corpus = None
        self.df_encoded = None
        self.directory = directory
        Path(self.directory).mkdir(exist_ok=True, parents=True)

    def _corpus_from_dicom(self):
        """
        Reads DICOM file and extracts the tag data. The 'sentences' are appended to the corpus. In the case of
        training also stores the classes.
        """
        self.corpus, self.y = [], []
        for n, s in self.df.iterrows():
            img_path = s["path"]
            try:
                ds = dcmread(img_path, force=True)
                if self.tag in ds:
                    sentence = str(ds.get(self.tag, "")).lower().replace("_", " ")
                else:
                    sentence = ""
                self.corpus.append(sentence)
                if "y" in s:
                    self.y.append(s["y"])
            except struct.error:
                continue
            except OSError:
                continue
        return self

    def _corpus_from_file(self):
        """
        Extracts the tag data. The 'sentences' are appended to the corpus. In the case of training also store the
        classes.
        """
        self.corpus = [i.lower().replace("_", " ")
                       for i in self.df[self.tag].values]
        if "y" in self.df.columns:
            self.y = self.df["y"].values
        return self

    def create_corpus(self):
        """
        Create corpus based on the format of the input dataset. If the column 'path' is present, we assume that
        the path points to an actual DICOM file. If not, we parse the data from a column denoted as the tag we are
        going to extract.
        """
        if "path" in self.df.columns:
            return self._corpus_from_dicom()
        else:
            return self._corpus_from_file()

    def encode(self):
        """Encode the corpus. All sentences are separated by words or individual numbers."""
        if self.corpus is None:
            raise ValueError("Create corpus before data encoding!")
        self.vectorizer = CountVectorizer(token_pattern=r"\b[\w\d\.]+\b")
        self.X = self.vectorizer.fit_transform(self.corpus)
        return self

    def dump_vectorizer(self):
        """Dump the trained vectorizer."""
        if self.vectorizer is None:
            raise ValueError("Encode data before dumping the vectorizer!")
        dump(self.vectorizer, self.directory + f"vectorizer.joblib")
        return self

    def dump_dataset(self):
        """Dump the encoded dataset."""
        if self.X is None or self.vectorizer is None:
            raise ValueError("Encode data before dumping the dataset!")
        self.df_encoded = pd.DataFrame.sparse.from_spmatrix(
            self.X, columns=self.vectorizer.get_feature_names_out())
        if self.y is not None:
            self.df_encoded["y"] = self.y
        self.df_encoded.to_csv(self.directory + "dataset.csv")
        return self


class ParamEncoder:

    sequence_variants = ["SK", "MTC", "SS", "TRSS", "SP", "MP", "OSP", "TOF", "NONE"]
    sequence_variants_cols = [f"seq_var_{i}" for i in sequence_variants]
    scan_options = ["PER", "RG", "CG", "PPG", "FC", "PFF", "PFP", "SP", "FS"]
    scan_options_cols = [f"scan_opt_{i}" for i in scan_options]
    scanning_sequence = ["SE", "IR", "GR", "EP", "RM"]
    scanning_sequence_cols = [f"scan_seq_{i}" for i in scanning_sequence]
    image_type = ["ORIGINAL", "DERIVED", "PRIMARY", "SECONDARY"]
    image_type_cols = [f"img_type_{i}" for i in image_type]
    photometric_interpretation = ["RGB", "MONOCHROME1", "MONOCHROME2"]
    photometric_interpretation_cols = [f"pho_intp_{i}" for i in photometric_interpretation]

    def get_sequence_variant(self, sv):
        """Encode SequenceVariant as binary vector. The zero vector encodes unknown are missing values."""
        res = [0] * len(self.sequence_variants)
        if pd.isna(sv):
            return res
        else:
            if type(sv) != MultiValue:
                sv = [sv]
            for _sv in sv:
                idx = self.sequence_variants.index(_sv)
                res[idx] = 1
            return res

    def get_scan_options(self, so):
        """Encode ScanOptions as binary vector. The zero vector encodes unknown are missing values."""
        res = [0] * len(self.scan_options)
        if pd.isna(so):
            return res
        else:
            for _so in so:
                if _so in self.scan_options:
                    idx = self.scan_options.index(_so)
                    res[idx] = 1
            return res

    def get_scanning_sequence(self, ss):
        """Encode ScanningSequence as binary vector. The zero vector encodes unknown are missing values."""
        res = [0] * len(self.scanning_sequence)
        if pd.isna(ss):
            return res
        else:
            if type(ss) != MultiValue:
                ss = [ss]
            for _ss in ss:
                idx = self.scanning_sequence.index(_ss)
                res[idx] = 1
            return res

    def get_image_type(self, it):
        """Encode ImageType as binary vector. The zero vector encodes unknown are missing values."""
        res = [0] * len(self.image_type)
        if pd.isna(it):
            return res
        else:
            for _it in it:
                if _it in self.image_type:
                    idx = self.image_type.index(_it)
                    res[idx] = 1
            return res

    def get_photometric_interpretation(self, pi):
        """Encode PhotometricInterpretation as binary vector. The zero vector encodes unknown are missing values."""
        res = [0] * len(self.photometric_interpretation)
        if pd.isna(pi):
            return res
        else:
            idx = self.photometric_interpretation.index(pi)
            res[idx] = 1
            return res

    def __init__(self, df, directory, modality="CT"):
        """
        A class to encode DICOM tags as numerical vectors. Supported tags are SequenceVariant, ScanOptions,
        ScanningSequence, ImageType, PhotometricInterpretation, and ContrastBolusAgent. Based on the work of
        Gauriau et al. (2020). More information can be found at https://doi.org/10.1007%2Fs10278-019-00308-x.
        """
        self.df = df
        self.modality = modality
        self.X = None
        self.y = None
        self.cols = ["contrast_bolus_agent"]
        self.df_encoded = None
        for c in [self.sequence_variants_cols, self.scan_options_cols, self.scanning_sequence_cols,
                  self.image_type_cols, self.photometric_interpretation_cols]:
            self.cols.extend(c)
        self.directory = directory
        Path(self.directory).mkdir(exist_ok=True, parents=True)

    def encode(self):
        """
        Reads the DICOM files and parses the tags to get a binary representation of the data. In the case of
        training also stores the classes.
        """
        rows, self.y = [], []
        for n, s in self.df.iterrows():
            img_path = s["path"]
            try:
                dataset = dcmread(img_path, force=True)
            except struct.error:
                continue
            except OSError:
                continue
            res = []
            contrast_bolus_agent = dataset.get("ContrastBolusAgent", np.nan)
            res.append(0 if pd.isna(contrast_bolus_agent) else 1)
            res.extend(self.get_sequence_variant(dataset.get("SequenceVariant", np.nan)))
            res.extend(self.get_scan_options(dataset.get("ScanOptions", np.nan)))
            res.extend(self.get_scanning_sequence(dataset.get("ScanningSequence", np.nan)))
            res.extend(self.get_image_type(dataset.get("ImageType", np.nan)))
            res.extend(self.get_photometric_interpretation(
                dataset.get("PhotometricInterpretation", np.nan)))
            rows.append(res)
            if "y" in s:
                self.y.append(s["y"])
        df_ml = pd.DataFrame(rows, columns=self.cols)
        self.X = df_ml.values
        return self

    def dump_dataset(self):
        """Dump the encoded dataset."""
        if self.X is None or self.y is None:
            raise ValueError("Encode data before dumping the dataset!")
        self.df_encoded = pd.DataFrame(self.X, columns=self.cols)
        if self.y is not None:
            self.df_encoded["y"] = self.y
        self.df_encoded = self.df_encoded.dropna()
        self.df_encoded.to_csv(self.directory + "dataset.csv")
        return self


class ParamEncoderMR(ParamEncoder):

    mr_acquisition_type = ["2D", "3D"]
    mr_acquisition_type_cols = [f"mr_acq_{i}" for i in mr_acquisition_type]

    def get_mr_aquisition_type(self, mat):
        """Encode MRAcquisitionType as binary vector. The zero vector encodes unknown are missing values."""
        res = [0] * len(self.mr_acquisition_type)
        if pd.isna(mat) or mat == "":
            return res
        else:
            idx = self.mr_acquisition_type.index(mat)
            res[idx] = 1
            return res

    def __init__(self, df, directory):
        """
        A class to encode DICOM tags as numerical vectors. Supported tags are SequenceVariant, ScanOptions,
        ScanningSequence, ImageType, PhotometricInterpretation, ContrastBolusAgent, EchoTime", EchoTrainLength,
        RepetitionTime, PixelSpacing, and SliceThickness. Based on the work of Gauriau et al. (2020). More
        information can be found at https://doi.org/10.1007%2Fs10278-019-00308-x.
        """
        super().__init__(df, modality="MR", directory=directory)
        self.cols_mr = [
            "echo_time", "echo_train_length", "repetition_time",
            "pixel_spacing", "slice_thickness"]
        for c in [self.mr_acquisition_type_cols]:
            self.cols_mr.extend(c)

    def encode(self):
        """
        Reads the DICOM files and parses the tags to get a binary representation of the data. In the case of
        training also stores the classes.
        """
        rows, y = [], []
        for n, s in self.df.iterrows():
            img_path = s["path"]
            try:
                dataset = dcmread(img_path, force=True)
            except struct.error:
                continue
            except OSError:
                continue
            res = []
            res.append(dataset.get("EchoTime", np.nan))  # 0/1064
            res.append(dataset.get("EchoTrainLength", np.nan))  # 0
            res.append(dataset.get("RepetitionTime", np.nan))  # 0
            res.append(dataset.get("PixelSpacing", [np.nan, np.nan])[0])
            res.append(dataset.get("SliceThickness", np.nan))
            res.extend(self.get_mr_aquisition_type(dataset.get("MRAcquisitionType", np.nan)))
            rows.append(res)
            if "y" in s:
                y.append(s["y"])
        df_ml = pd.DataFrame(rows, columns=self.cols_mr)
        super().encode()
        df_default = pd.DataFrame(self.X, columns=self.cols)
        df_tmp = pd.concat([df_default, df_ml], axis=1)
        df_tmp["y"] = y
        df_tmp = df_tmp.dropna()
        self.X, self.y = df_tmp.iloc[:, :-1].values, df_tmp["y"].values
        self.cols = self.cols + self.cols_mr
        return self
