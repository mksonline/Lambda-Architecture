package object model {

    case class ProductSale(Id: Int, firstName: String, lastName: String, house: Int, street: String, city: String, state: String, zip: String, prod: String, tag: String) extends Serializable

    case class ProvinceSale(prod: String, state: String, total: Long) extends Serializable

}
