package com.starfox.sparkaid

import java.util.regex.Pattern

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, expr, explode_outer => explode}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import scala.collection.mutable.ListBuffer

object NestedSchemaHandler {
  def apply(separator: String, arrayNotation: String, fieldNameNormalizer: String => String): NestedSchemaHandler = new NestedSchemaHandler(separator, arrayNotation, fieldNameNormalizer)
  def apply(separator: String, arrayNotation: String): NestedSchemaHandler = new NestedSchemaHandler(separator, arrayNotation)
  def apply(): NestedSchemaHandler = new NestedSchemaHandler()
}

class NestedSchemaHandler(val separator:String = "___", val arrayDenotation: String = "_ARRAY", val fieldNameNormalizer: String => String = x => x ) {
  private val actualArrayDenotation = arrayDenotation + separator

  private val safeFieldNameNormalizer: String => String = (raw: String) => {
    val ret = fieldNameNormalizer (raw)
    if (ret.contains(separator) || ret.endsWith(arrayDenotation))
      println(s"WARNING: The normalized field name ($ret) contains the special string ($separator/$arrayDenotation)")
    ret
  }

  def flatten(df: DataFrame): DataFrame = {
    val cols: Array[Column] = getFieldsInfoForFlattening(df.schema).map(
      field => expr(field.map(chunk => chunk.map(segment => f"`$segment`").mkString("."))
        .grouped(2).map(_.mkString("."))
        .reduceLeft((left, right) => s"transform($left, x -> x.$right)"))
        .alias(buildFlattenedFieldName(field))
    )
    df.select(cols:_*)
  }

  final def flattenAndExplode(df: DataFrame): DataFrame = {
    innerFlattenAndExplode(df)
  }

  @scala.annotation.tailrec
  private def innerFlattenAndExplode(df: DataFrame, normalizeRootSegment: Boolean = true): DataFrame = {
    val cols: Vector[Column] = getFieldsInfoForFlattening(df.schema, includeArray = false)
      .foldLeft((Vector.empty[Column], false))((cols, i) => {
        i match {
          case chunk +: Seq() =>
            (cols._1 :+ expr(chunk.map(segment => f"`$segment`").mkString(".")).alias(buildFlattenedChunkName(chunk, normalizeRootSegment)),
              cols._2)
          case chunk +: tail =>
            val curCol =
              if (!cols._2) explode(expr(chunk.map(chunk => f"`$chunk`").mkString("."))).alias(buildFlattenedChunkName(chunk, normalizeRootSegment) + (if (tail.isEmpty) "" else this.arrayDenotation))
              else expr(chunk.map(chunk => f"`$chunk`").mkString(".")).alias(buildFlattenedChunkName(chunk, normalizeRootSegment))
            (cols._1 :+ curCol, true)
        }
      })._1

    val ret = df.select(cols:_*)

    ret.schema.count(_.dataType match {
      case (_: StructType | _: ArrayType) => true
      case _ => false
    }) match {
      case 0 => ret
      case _ => innerFlattenAndExplode(ret, false)
    }
  }

  def unflatten(df: DataFrame): DataFrame = {
    // build a SchemaNode object
    val schema = SchemaNode("root")
    df.schema.fields.map(f => (f, f.name)).foreach(f => schema.addChild(f._1))

    val cols: Array[Column] = schema.value match {
      case Right(fields) => fields.map(f => f.value match {
        case Left(f) => col(f.name)
        case Right(_) => expr(f.constructUnflattenExpr).cast(f.toSparkDataType.dataType).alias(f.segmentName)
      }).toArray

      case _ => throw new Exception("the code should never reach this point")
    }

    df.select(cols:_*)
  }

  /** This recursive function reads the nested schema, breaks down each root-leaf path into one 2-dimension collection of String.
   *  Each (inner) element in the collection is one chunk of the nested schema. That big collection is broken down into segments separated by an ArrayType.
   *  E.g:
   *    For the root-leaf path (structA -> structB -> structC -> stringD), the output element is a 2-d collection,
   *      with the outer layer has only one element, which in turn consists of 4 element
   *    For the root-leaf path (structA -> arrayB(struct) -> structC -> structD -> integerE), the output element is a 2-d collection,
   *      Depending on `explodeArrayType`, if true, then the outer layer has 2 elements, one has 2 elements (A, B) and the other has 3 (C, D, E)
   *      otherwise, the outer layer would have only one element: (A, B) */
  private def getFieldsInfoForFlattening(dtype: DataType, name: Vector[String] = Vector.empty, includeArray: Boolean = true): Array[Vector[Vector[String]]] = {
    dtype match {
      case st: StructType =>
        st.fields.flatMap(field => getFieldsInfoForFlattening(field.dataType, Vector(field.name), includeArray).map (
          child => (name ++ child.head) +: child.tail ))

      case ar: ArrayType =>
        if (includeArray)
          ar.elementType match {
            case e @ (_: StructType | _: ArrayType) =>
              getFieldsInfoForFlattening(e).map(Vector(name) ++ _)
            case _ => Array(Vector(name))
          }
        else Array(Vector(name) ++ Vector(Vector.empty[String]))

      case _ => Array(Vector(name))
    }
  }

  private def buildFlattenedChunkName(raw: Vector[String], normalizeRootSegment: Boolean = true): String = {
    if (normalizeRootSegment)
      raw.map(safeFieldNameNormalizer).mkString(separator)
    else
      (raw.head +: raw.tail.map(safeFieldNameNormalizer)).mkString(separator)
  }

  private def buildFlattenedFieldName(raw: Vector[Vector[String]]): String = {
    raw.map(buildFlattenedChunkName(_)).mkString(actualArrayDenotation)
  }

  private class SchemaNode(val segmentName: String, val value: Either[StructField, ListBuffer[SchemaNode]], val isArray: Boolean = false) {
    def addChild(rawField: StructField, path: Vector[String] = Vector.empty):SchemaNode = {
      (value, if (path.isEmpty) rawField.name.split(Pattern.quote(separator)).toVector else path) match {
        // parent is a leaf, then could not add - just throw an exception
        case (Left(_), _) => throw new Exception("Leaf node cannot have a child")
        // adding a leaf to children list
        case (Right(children), leafName +: Seq()) =>
          children.count(_.segmentName.equals(leafName)) match {
            case 0 => children.append(SchemaNode(leafName, rawField))
            case _ => throw new Exception(s"Duplicated field name found: `$leafName`")
          }
        // adding a subtree children list
        case (Right(children), chunk +: tail) =>
          val (isArray, fieldName) =
            if (chunk.endsWith(arrayDenotation)) (true, chunk.dropRight(arrayDenotation.length))
            else (false, chunk)
          val curSubTree: SchemaNode = children.find(_.segmentName.equals(fieldName)) match {
            // if an existing nested tree is found, then adding the new field to this tree
            case Some(c) =>
              // if the existing nested tree is an array, then the new field must also be an array, and vice versa
              if (c.isArray != isArray) throw new Exception(s"Inconsistency in nested array denotation in the field $fieldName")
              c
            // if no existing nested tree is found, then create a new one and add to the current children list
            case _ =>
              val tmp = SchemaNode(fieldName, isArray)
              children.append(tmp)
              tmp
          }
          // add child to the subtree
          if (curSubTree.isArray) {
            rawField.dataType match {
              case arrType: ArrayType => curSubTree.addChild(rawField.copy(dataType = arrType.elementType), tail)
              case _ => throw new Exception(s"The nested array field ${curSubTree.segmentName} contains a non-arrayType ${tail.head}")
            }
          } else
            curSubTree.addChild(rawField, tail)
      }
      this
    }

    def constructUnflattenExpr: String = {
      value match {
        case Left(t) => s"`${t.name}`"
        case Right(children) =>
          s"${if (isArray) "arrays_zip" else "struct"}(${children.map(_.constructUnflattenExpr).mkString(", ")})"
      }
    }

    def toSparkDataType: StructField = {
      value match {
        case Left(t) => t.copy(name = this.segmentName)
        case Right(children) =>
          val subType = StructType(children.map(_.toSparkDataType))
          if (isArray) StructField(segmentName, ArrayType(subType))
          else StructField(segmentName, subType)
      }
    }

    def printSchema(level:Int = 0): Unit = {
      for(i <- 0 until level) { print("|   ")}
      print(f"|-- $segmentName")
      if (isArray) print(" - Array of")
      value match {
        case Left(f) => println(f" (${f.dataType})")
        case Right(t) =>
          println()
          t.foreach(c => c.printSchema(level + 1))
      }
    }
  }

  private object SchemaNode {
    def apply(name: String, leaf: StructField): SchemaNode = new SchemaNode(name, Left(leaf), false)
    def apply(name: String, isArray: Boolean = false): SchemaNode = new SchemaNode(name, Right(ListBuffer.empty[SchemaNode]), isArray)
  }
}
