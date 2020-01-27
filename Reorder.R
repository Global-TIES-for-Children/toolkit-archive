
## Create support function to reorder variables
ReOrder<-function(Data,MoveItem,Previous){
  position1<-which(names(Data)==Previous)
  position2<-which(names(Data)==MoveItem)
  if(position2> position1){
    NewOrder<-c(1:position1,position2,((position1+1):length(Data))[-which(((position1+1):length(Data))==position2)])
  }else{
    NewOrder<-c((1:position1)[-position2],position2,(position1+1):length(Data))
  }
  
  Data<-Data[,NewOrder]
  return(Data)
}

## Mayari M.