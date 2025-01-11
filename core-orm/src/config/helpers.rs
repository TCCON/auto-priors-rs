use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::{fmt::Debug, path::PathBuf};
use std::str::FromStr;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug)]
pub enum YesOrNo<T: Debug> {
    Yes(T),
    No
}

impl<T> Clone for YesOrNo<T> where T: Clone + Debug {
    fn clone(&self) -> Self {
        match self {
            Self::Yes(arg0) => Self::Yes(arg0.clone()),
            Self::No => Self::No,
        }
    }
}

impl<'de, T> Deserialize<'de> for YesOrNo<T> where T: FromStr + Debug {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de> {
        let s = String::deserialize(deserializer)?;
        Self::from_str(&s)
            .map_err(|e| serde::de::Error::custom("Could not convert string to YesOrNo"))
    }
}

impl<T> Serialize for YesOrNo<T> where T: Serialize + Debug {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer {
        match self {
            YesOrNo::Yes(v) => v.serialize(serializer),
            YesOrNo::No => "NO".serialize(serializer),
        }
    }
}

impl<T> FromStr for YesOrNo<T> where T: FromStr + Debug {
    type Err = T::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "no" => Ok(Self::No),
            _ => T::from_str(s).map(Self::Yes)
        }
    }
}

impl<T> ToString for YesOrNo<T> where T: ToString + Debug {
    fn to_string(&self) -> String {
        match self {
            YesOrNo::Yes(v) => v.to_string(),
            YesOrNo::No => "NO".to_string(),
        }
    }
}


pub(super) fn deserialize_subfile<'de, D, T>(deserializer: D, description: &str) -> Result<T, D::Error>
where 
    D: Deserializer<'de>,
    T: DeserializeOwned
{
    let p = PathBuf::deserialize(deserializer)?;
    let mut f = File::open(p)
        .map_err(|e| serde::de::Error::custom(format!(
            "Error opening {description} TOML file: {e}"
        )))?;
    let mut toml_str = String::new();
    f.read_to_string(&mut toml_str)
        .map_err(|e| serde::de::Error::custom(format!(
            "Error reading {description} TOML file: {e}"
        )))?;
    toml::from_str(&toml_str)
        .map_err(|e| serde::de::Error::custom(format!(
            "Error deserializing {description} TOML file: {e}"
        )))
}

pub(super) fn serialize_subfile<S, T>(value: &T, serializer: S, p: &Path, description: &str) -> Result<S::Ok, S::Error>
where 
    S: Serializer,
    T: Serialize
{
    // First, make sure we can write the secondary file.
    let s = toml::to_string_pretty(value)
        .map_err(|e| serde::ser::Error::custom(format!(
            "Could not serialize {description}: {e}"
        )))?;
    let mut f = std::fs::File::create(&p)
        .map_err(|e| serde::ser::Error::custom(format!(
            "Could not open {} for writing: {e}", p.display()
        )))?;
    f.write_all(s.as_bytes())
        .map_err(|e| serde::ser::Error::custom(format!(
            "Could not write {description} to {}: {e}", p.display()
        )))?;

    let abs_p = p.canonicalize()
        .map_err(|e| serde::ser::Error::custom(format!(
            "Could not make path to {} file absolute: {e}", p.display()
        )))?;
    abs_p.serialize(serializer)
}
