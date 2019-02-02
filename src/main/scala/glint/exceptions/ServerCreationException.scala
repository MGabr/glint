package glint.exceptions

/**
  * An exception that occurs when server creation fails
  *
  * @param message A specific error message detailing what failed
  */
class ServerCreationException(message: String) extends Exception(message)
