-- Meta class
Rectangle = {}
Rectangle.__index = Rectangle

-- Derived class method new
function Rectangle:new (length, breadth)
   local this =
   {
       length = length or 0.0,
       breadth = breadth or 0.0,
       area = 0.0
   }
   this.area = this.length * this.breadth
   setmetatable(this, Rectangle)
   this.length = length or this.length
   this.breadth = breadth or this.breadth
   return this
end

-- Derived class method printArea
function Rectangle:getArea ()
   return self.area
end

-- Derived class method init
function Rectangle:init (length, breadth)
   self.length = length
   self.breadth = breadth
   self.area = length*breadth;
   return self
end

-- Derived class method get
function Rectangle:get ()
   return self
end