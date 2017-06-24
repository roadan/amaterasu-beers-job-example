val highNoDf = AmaContext.getDataFrame("start", "odd").where("number > 100")
highNoDf.write.mode(SaveMode.Overwrite).json(s"${env.outputRootPath}/dtatframe_res")
