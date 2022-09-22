//! The users module defines a set of structs that describe users who have permission to interact
//! with the database, as well as functionality for managing and authenticating users.

use std::fs::{create_dir, File};
use std::io::{Read, Write};
use nom::AsBytes;

use serde::{Serialize, Deserialize};
use sha2::{Sha256, Digest};

/// Default file system directory for saving user info.
const USERS_SAVE_PATH: &'static str = "./users";

/// Authentication methods supported for user accounts.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum AuthenticationMethod {
    /// No authentication is enabled. Not recommended.
    None,

    /// The user's authenticity may be proven via a password, hashed with SHA256.
    Password(String),
}


#[derive(Serialize, Deserialize)]
pub struct User {
    name: String,
    auth_method: AuthenticationMethod,
}

impl User {
    /// Creates a new user and saves it to disk.
    pub fn create(name: &str, password: Option<&str>) -> User {
        let auth_method = match password {
            None => AuthenticationMethod::None,
            Some(password) => AuthenticationMethod::Password(hash_sha256(password)),
        };

        let user = User { name: name.to_owned(), auth_method };
        user.save();
        user
    }

    /// Attempt to authenticate against an existing user.
    /// The password must be passed in already hashed with SHA256.
    pub fn authenticate<'a>(name: &'a str, password: Option<&str>) -> Result<User, &'a str> {
        match User::load(name) {
            None => { Err("User does not exist") }
            Some(user) => {
                match &user.auth_method {
                    AuthenticationMethod::None => Ok(user),
                    AuthenticationMethod::Password(user_pwd) => {
                        if password.is_none() {
                            return Err("Missing password");
                        }

                        match user_pwd == password.unwrap() {
                            true => Ok(user),
                            false => Err("Wrong user or password")
                        }
                    }
                }
            }
        }
    }

    /// Save a user to disk, overwriting any previous user data.
    /// TODO: maybe there should be a separate alter. Name shouldn't be overwritable to same file
    /// TODO: maybe we shouldn't be storing each user like this, as a separate file? idk
    fn save(&self) {
        create_dir(USERS_SAVE_PATH);

        let path = format!("{}/{}.txt", USERS_SAVE_PATH, self.name);
        let file = File::create(path);

        match file {
            Ok(mut f) => {
                let data = serde_yaml::to_string(&self).unwrap();
                f.write_all(data.as_bytes());
            }
            Err(err) => {
                dbg!(err);
            }
        };
    }

    /// Attempt to load an existing user from disk, based on name.
    fn load(name: &str) -> Option<User> {
        let path = format!("{}/{}.txt", USERS_SAVE_PATH, name);
        let file = File::open(path);

        match file {
            Ok(mut f) => {
                let mut data = String::new();
                f.read_to_string(&mut data).unwrap();

                let user: User = serde_yaml::from_str(&data).unwrap();
                Some(user)
            }
            Err(err) => {
                dbg!(err);
                None
            }
        }
    }
}


/// Hashes a given string using SHA256, and returns the result as a hex string.
fn hash_sha256(str: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(str.as_bytes());
    let hash = hasher.finalize();

    hex::encode(hash.as_bytes().to_vec())
}


#[cfg(test)]
mod tests {
    use crate::users::{AuthenticationMethod, hash_sha256, User};

    #[test]
    fn creates_users() {
        User::create("andres", None);
        User::create("brendan", Some("mysecurepassword"));
    }

    #[test]
    fn loads_users() {
        let user = User::load("andres").unwrap();
        assert_eq!(user.name, "andres");
        assert_eq!(user.auth_method, AuthenticationMethod::None);

        let user = User::load("brendan").unwrap();
        assert_eq!(user.name, "brendan");
        assert_eq!(user.auth_method, AuthenticationMethod::Password(hash_sha256("mysecurepassword")));
    }

    #[test]
    fn authenticates_users() {
        let user = User::authenticate("andres", None).unwrap();
        assert_eq!(user.name, "andres");
        assert_eq!(user.auth_method, AuthenticationMethod::None);

        let user = User::authenticate("brendan", None);
        assert!(user.is_err());

        let user = User::authenticate("brendan", Some(&hash_sha256("mysecurepassword"))).unwrap();
        assert_eq!(user.name, "brendan");
        assert_eq!(user.auth_method, AuthenticationMethod::Password(hash_sha256("mysecurepassword")));
    }
}