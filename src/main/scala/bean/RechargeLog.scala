package bean

case class RechargeLog(
                        bussinessRst: String,
                        channelCode: String,
                        chargefee: Double,
                        clientIp: String,
                        endReqTime: String,
                        orderId: String,
                        prodCnt: Int,
                        provinceCode: String,
                        requestId: String,
                        viceName: String,
                        shouldfee: Double,
                        startReqTime: String,
                        sysId: String
                      )
